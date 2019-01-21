package com.neighborhood.aka.laplace.estuary.core.snapshot

import com.neighborhood.aka.laplace.estuary.bean.exception.snapshot.{TimeOverdueException, UnknownSnapshotStatusException}
import com.neighborhood.aka.laplace.estuary.core.snapshot.SnapshotStateMachine.{DefaultSnapshotTask, DefaultSnapshotTaskInfo, SnapshotTask, SnapshotTaskInfo}
import com.neighborhood.aka.laplace.estuary.core.snapshot.SnapshotStatus.SnapshotStatus
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.snapshot.MysqlSnapshotStateMachine
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2018/7/9.
  */
trait SnapshotStateMachine[T] {
  /**
    *
    * 任务资源管理器
    *
    * @return
    */
  def taskManager: TaskManager

  /**
    * 同步任务id
    *
    * @return
    */
  def syncTaskId: String = taskManager.syncTaskId

  /**
    * 目标时间
    */
  private var targetTimestamp: Long = Long.MaxValue
  /**
    * snapshotTaskId
    */
  private var snapshotTaskId: String = ""
  /**
    * 日志
    */
  private val logger = LoggerFactory.getLogger(s"SnapshotStateMachine-$syncTaskId")
  /**
    * 当前状态
    */
  private var currentStatus: SnapshotStatus = SnapshotStatus.NO_SNAPSHOT


  /**
    * 上次同步的时间戳
    */
  @transient
  private var lastTimestamp: Long = System.currentTimeMillis()

  /**
    * 开始一个同步任务
    * 1.更新targetTimestamp
    * 2.更新snapshotTaskId
    * 3.更新lastTimestamp
    * 4.更新状态状态为ACCUMULATING_DATA
    *
    * @param snapshotTask
    */
  def startSnapshotTask(snapshotTask: SnapshotTask, currentTimestamp: => Long = System.currentTimeMillis()): Unit = {
    val targetTimestamp = snapshotTask.targetTimestamp
    val snapshotTaskId = snapshotTask.snapshotTaskId
    if (targetTimestamp < currentTimestamp) throw new TimeOverdueException(s"currentTimestamp:$currentTimestamp,is later than targetTimestamp:$targetTimestamp,id:$syncTaskId")
    setLastTimestamp(currentTimestamp)
    setTargetTimestamp(targetTimestamp)
    setSnapshotTaskId(snapshotTaskId)
    setStatusAndFlush(SnapshotStatus.ACCUMULATING_DATA)
    logger.info(s"start to accumulate data,id:$syncTaskId")
  }

  /**
    * 切换为Suspend时调用的方法
    */
  def onSuspend: Unit = {
    logger.info(s"switch to suspend,id:$syncTaskId")
    //这个不能调用AndFlush，防止刷新到数据库中
    setStatus(SnapshotStatus.SUSPEND_4_WAKEUP)
    flushIntoTaskManager(SnapshotStatus.SUSPEND_4_WAKEUP)
  }

  /**
    * 结束任务时调用的方法
    */
  def onTerminatingTask: Unit = {
    logger.info(s"task:snapshot-${getTargetTimestamp} is done")
    setLastTimestamp(-1)
    setTargetTimestamp(Long.MaxValue)
    setSnapshotTaskId("")
    setStatusAndFlush(SnapshotStatus.NO_SNAPSHOT)

  }

  /**
    * 判断数据是否到位
    *
    * @param t
    * @return
    */
  def judgeSnapshotData(t: Option[T]): Boolean

  /**
    * 是否是在运行快照任务
    *
    * @return
    */
  def hasRunningTask: Boolean = if (getStatus.equals(SnapshotStatus.UNKNOWN)) throw new UnknownSnapshotStatusException(s"the status is Unknown when judge is Running task,syncTaskId:${syncTaskId},snapshotId:$snapshotTaskId") else !getStatus.equals(SnapshotStatus.NO_SNAPSHOT)

  /**
    * 是不是挂起等待唤醒
    *
    * @return
    */
  def isSuspend: Boolean = if (getStatus.equals(SnapshotStatus.UNKNOWN)) throw new UnknownSnapshotStatusException(s"the status is Unknown when judge is Running task,syncTaskId:${syncTaskId},snapshotId:$snapshotTaskId") else getStatus.equals(SnapshotStatus.SUSPEND_4_WAKEUP)

  /**
    * 构造任务
    *
    * @return
    */
  def buildSnapshotTask: SnapshotTask = DefaultSnapshotTask(syncTaskId, snapshotTaskId, targetTimestamp)

  /**
    * 构造任务信息
    *
    * @return
    */
  def buildSnapshotTaskInfo: SnapshotTaskInfo = DefaultSnapshotTaskInfo(buildSnapshotTask.asInstanceOf[DefaultSnapshotTask], getStatus, lastTimestamp)

  /**
    * 根据 SnapshotTaskInfo 还原状态
    *
    * @param info
    * @return
    */
  def restoreStateMachineFromSnapshotInfo(info: SnapshotTaskInfo): SnapshotStateMachine[T] = {
    lazy val targetTimestamp = info.targetTimestamp
    lazy val snapshotTaskId = info.snapshotTaskId
    lazy val status = info.status
    lazy val lastTimestamp = if (status.equals(SnapshotStatus.ACCUMULATING_DATA)) System.currentTimeMillis() else info.lastTimestamp
    setTargetTimestamp(targetTimestamp)
    setSnapshotTaskId(snapshotTaskId)
    setLastTimestamp(lastTimestamp)
    this.setStatus(status)
    this
  }

  protected def flushIntoTaskManager(status: => SnapshotStatus) = taskManager.snapshotStauts.set(
    s"""
    {
      "syncTaskId" : "$syncTaskId",
      "snapshotTaskId" : "$snapshotTaskId",
      "status" : "${status}",
      "targetTimestamp":$targetTimestamp,
      "lastTimestamp":$lastTimestamp
    }
    """.stripMargin
  )

  protected def flushTaskInfoIntoMap = MysqlSnapshotStateMachine.flushTaskInfo(syncTaskId, buildSnapshotTaskInfo)

  protected def flushTaskInfoIntoDatabase = ???

  /**
    * 向taskManager刷新状态
    *
    * @param status
    */
  protected def flushStatus(status: => SnapshotStatus = currentStatus) = {

    flushIntoTaskManager(status)
  }

  /**
    * 是否完成
    * 只是简单比较时间戳
    *
    * @param timestamp
    * @return
    */
  protected def snapshotIsDone(timestamp: Long): Boolean = timestamp >= targetTimestamp


  protected def getStatus: SnapshotStatus = this.currentStatus

  protected def setStatus(status: SnapshotStatus): Unit = {
    this.currentStatus = status

  }

  protected def setStatusAndFlush(status: SnapshotStatus): Unit = this.currentStatus = {
    flushStatus(status);
    status
  }

  protected def getLastTimestamp: Long = this.lastTimestamp

  protected def getTargetTimestamp: Long = this.targetTimestamp

  protected def getSnapshotTaskId: String = this.snapshotTaskId

  protected def setLastTimestamp(ts: Long): Unit = {
    this.lastTimestamp = ts
  }

  protected def setLastTimestampAndFlush(ts: Long): Unit = {
    setLastTimestamp(ts)
    flushStatus()
  }

  protected def setTargetTimestamp(ts: Long): Unit = {
    this.targetTimestamp = ts
  }

  protected def setTargetTimestampAndFlush(ts: Long): Unit = {
    setTargetTimestamp(ts)
    flushStatus()
  }

  protected def setSnapshotTaskId(id: String): Unit = {
    this.snapshotTaskId = id
  }

  protected def setSnapshotTaskIdAndFlush(id: String): Unit = {
    setSnapshotTaskId(id)
    flushStatus()
  }
}

object SnapshotStateMachine {

  sealed trait SnapshotTask {
    def syncTaskId: String

    def snapshotTaskId: String

    def targetTimestamp: Long
  }

  case class DefaultSnapshotTask(syncTaskId: String, snapshotTaskId: String, targetTimestamp: Long) extends SnapshotTask

  case class Mysql2KafkaSnapshotTask(syncTaskId: String, snapshotTaskId: String, targetTimestamp: Long, kafkaPartition: Int) extends SnapshotTask

  sealed trait SnapshotTaskInfo {
    def snapshotTask: SnapshotTask

    def status: SnapshotStatus.SnapshotStatus

    def lastTimestamp: Long

    def targetTimestamp = snapshotTask.targetTimestamp

    def snapshotTaskId = snapshotTask.snapshotTaskId

    def syncTaskId = snapshotTask.syncTaskId
  }

  case class DefaultSnapshotTaskInfo(
                                      snapshotTask: DefaultSnapshotTask,
                                      status: SnapshotStatus.SnapshotStatus = SnapshotStatus.UNKNOWN,
                                      lastTimestamp: Long = System.currentTimeMillis
                                    ) extends SnapshotTaskInfo

  case class Mysql2KafkaSnapshotTaskInfo(
                                          snapshotTask: Mysql2KafkaSnapshotTask,
                                          status: SnapshotStatus.SnapshotStatus = SnapshotStatus.UNKNOWN,
                                          lastTimestamp: Long = System.currentTimeMillis
                                        ) extends SnapshotTaskInfo {
    def kafkaPartitionNum: Int = snapshotTask.kafkaPartition
  }

  /** ********************************************************************/
  sealed trait SnapshotTaskDataSendFinish[+T <: SnapshotTaskInfo] {
    def snapshotTaskInfo: T

    def finishTimstamp = finishTimstamp0

    lazy val finishTimstamp0 = System.currentTimeMillis()
  }

  case class Mysql2KafkaSnapshotTaskDataSendFinish(
                                                    snapshotTaskInfo: Mysql2KafkaSnapshotTaskInfo
                                                  ) extends SnapshotTaskDataSendFinish[Mysql2KafkaSnapshotTaskInfo]

}