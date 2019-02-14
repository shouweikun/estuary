package com.neighborhood.aka.laplace.estuary.mysql.snapshot

import java.util.concurrent.ConcurrentHashMap

import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.bean.exception.snapshot.UnmatchedTaskInfoException
import com.neighborhood.aka.laplace.estuary.core.snapshot.SnapshotStateMachine
import com.neighborhood.aka.laplace.estuary.core.snapshot.SnapshotStateMachine.{Mysql2KafkaSnapshotTask, Mysql2KafkaSnapshotTaskInfo, SnapshotTaskInfo}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.utils.CanalEntryTransHelper
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2018/7/9.
  * 负责快照的整个生命周期
  */
class MysqlSnapshotStateMachine(
                                 val taskManager: TaskManager
                               ) extends SnapshotStateMachine[CanalEntry.Entry] {

  private var kafkaPartitionNum: Int = -1
  /**
    * 同步任务Id
    */
  override val syncTaskId = taskManager.syncTaskId
  /**
    * 日志
    */
  private val logger = LoggerFactory.getLogger(s"MysqlSnapshotStateMachine-$syncTaskId")

  flushIntoTaskManager(getStatus)

  override def startSnapshotTask(snapshotTask: SnapshotStateMachine.SnapshotTask, currentTimestamp: => Long = System.currentTimeMillis()): Unit = {
    if (!hasRunningTask) {
      super.startSnapshotTask(snapshotTask, currentTimestamp)
      kafkaPartitionNum = snapshotTask.asInstanceOf[Mysql2KafkaSnapshotTask].kafkaPartition
      logger.info(s"startSnapshotTask success,$snapshotTask")
    } else {
      logger.warn(s"is running task:${this.buildSnapshotTaskInfo},so cannot submit task :$snapshotTask,id:$syncTaskId")
    }
  }

  /**
    * 在父类方法的基础上
    * 增加kafkaPartition数量的更新
    *
    * @param info
    * @return
    */
  override def restoreStateMachineFromSnapshotInfo(info: SnapshotStateMachine.SnapshotTaskInfo): MysqlSnapshotStateMachine = {
    logger.info(s"start restore State from SnapshotTaskInfo:$info,id:$syncTaskId")
    if (!info.isInstanceOf[Mysql2KafkaSnapshotTaskInfo]) throw new UnmatchedTaskInfoException(s"need Mysql2KafkaSnapshotTaskInfo but $info found when restoreStateMachineFromSnapshotInfo in MysqlSnapshotStateMachine ,id:$syncTaskId")
    lazy val theInfo = info.asInstanceOf[Mysql2KafkaSnapshotTaskInfo]
    setkafkaPartitionNum(theInfo.kafkaPartitionNum)
    super.restoreStateMachineFromSnapshotInfo(info).asInstanceOf[MysqlSnapshotStateMachine]

  }

  override def buildSnapshotTask: SnapshotStateMachine.Mysql2KafkaSnapshotTask = Mysql2KafkaSnapshotTask(syncTaskId, getSnapshotTaskId, getTargetTimestamp, getkafkaPartitionNum)

  /**
    * 判断数据是否达到快照节点
    * 判断条件
    * 1.数据为空时，距离上次拉取到的数据的ExecuteTime相差3min并且当前时间比target时间后1min还大
    * 2.数据不空时, 当前数据的executeTime大于target时间+1min
    *
    * @param ts
    * @return
    */
  private[snapshot] def entryTsIsQualified(ts: Option[Long]): Boolean = ts.fold {
    lazy val now = System.currentTimeMillis()
    lazy val isOver = now - 1000 * 60 * 3 >= getLastTimestamp && now - 1000 * 60 >= getTargetTimestamp
    if (isOver) logger.info(s"snapshot is done cause now:$now is 3 min later than lastTimestamp:$getLastTimestamp,targetTimestamp:$getTargetTimestamp") else {
      logger.info(s"check entry is Qualified by check command,id:$syncTaskId")
    }
    isOver
  }(_ > getTargetTimestamp)

  /**
    * 对外暴露的判断数据是否达到快照节点的方法
    * 当没完成条件且数据不空时，刷新lastTimestamp
    *
    * @param entryOption
    * @return
    */
  def judgeSnapshotData(entryOption: Option[CanalEntry.Entry]): Boolean = {
    lazy val ts = entryOption.map(_.getHeader.getExecuteTime)
    val re = this.hasRunningTask && entryTsIsQualified(ts)
    if (re) {
      logger.info(s"accumulating data is done,entry:${
        entryOption
          .map(_.getHeader)
          .map(CanalEntryTransHelper.headerToJson(_))
      },id:$syncTaskId")
    } else entryOption
      .map(_.getHeader.getExecuteTime)
      .map(setLastTimestampAndFlush(_))
    re
  }

  /**
    * 构造任务信息
    *
    * @return
    */
  override def buildSnapshotTaskInfo: Mysql2KafkaSnapshotTaskInfo = {
    Mysql2KafkaSnapshotTaskInfo(buildSnapshotTask, getStatus, getLastTimestamp)
  }

  protected def getkafkaPartitionNum = this.kafkaPartitionNum

  protected def setkafkaPartitionNum(kafkaPartitionNum: Int) = this.kafkaPartitionNum = kafkaPartitionNum
}

object MysqlSnapshotStateMachine {
  private val logger = LoggerFactory.getLogger("MysqlSnapshotStateMachine")
  private val snapshotTaskMap: ConcurrentHashMap[String, SnapshotTaskInfo] = new ConcurrentHashMap

  def fromExternalStorage(taskManager: TaskManager): MysqlSnapshotStateMachine = ???

  def fromExistingTask(taskManager: TaskManager): MysqlSnapshotStateMachine = {
    lazy val syncTaskId = taskManager.syncTaskId
    logger.info(s"start get stateMachine from Existing Task,id:$syncTaskId")
    //todo 从数据库里获得状态
    lazy val re = getValueFromSnapshotTaskMap(taskManager).getOrElse {
      logger.info(s"do not found stateMachine in ExistingTask,build a new stateMachine,id:$syncTaskId")
      lazy val stateMachine = MysqlSnapshotStateMachine(taskManager)
      putIntoSnapshotTaskMap(taskManager, stateMachine)
      stateMachine
    }
    //一定要flush 一下
    re.flushStatus()
    re
  }


  def flushTaskInfo(syncTaskId: String, taskInfo: SnapshotTaskInfo): Unit = {

    snapshotTaskMap.put(syncTaskId, taskInfo)
  }

  private def apply(
                     taskManager: TaskManager
                   ): MysqlSnapshotStateMachine = {
    lazy val re = new MysqlSnapshotStateMachine(taskManager)
    //    putIntoSnapshotTaskMap(taskManager, re.buildSnapshotTaskInfo)
    re
  }

  private def putIntoSnapshotTaskMap(taskManager: TaskManager, stateMachine: MysqlSnapshotStateMachine): Unit = {
    lazy val syncTaskId = taskManager.syncTaskId
    lazy val taskInfo = stateMachine.buildSnapshotTaskInfo
    snapshotTaskMap.put(syncTaskId, taskInfo)
    logger.info(s"put new taskInfo:$taskInfo into map,id:$syncTaskId")
  }

  private def getValueFromSnapshotTaskMap(taskManager: TaskManager): Option[MysqlSnapshotStateMachine] = {
    lazy val syncTaskId = taskManager.syncTaskId
    lazy val stateMachine = MysqlSnapshotStateMachine(taskManager)
    Option(snapshotTaskMap.get(syncTaskId)).map(stateMachine.restoreStateMachineFromSnapshotInfo(_))
  }
}
