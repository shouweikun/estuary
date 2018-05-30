package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{OneForOneStrategy, Props}
import com.neighborhood.aka.laplace.estuary.bean.exception.sink.SinkDataException
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SinkerMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager
import org.I0Itec.zkclient.exception.ZkTimeoutException
import org.springframework.util.StringUtils

/**
  * Created by john_liu on 2018/5/8.
  */
class MysqlBinlogInOrderSinkerManager(
                                       val taskManager: Mysql2KafkaTaskInfoManager
                                     ) extends SourceDataSinkerManagerPrototype {


  /**
    * 是否是顶部
    */
  override val isHead: Boolean = true
  /**
    * 同步任务ID
    */
  override val syncTaskId = taskManager.syncTaskId
  /**
    * sinker的数量
    */
  val sinkerNum = taskManager.sinkerNum
  /**
    * sinker的list
    */
  lazy val sinkList = context.children.toList
  /**
    * 计数器
    */
  lazy val processingCounter = taskManager.processingCounter
  /**
    * 是否计数
    */
  val isCounting = taskManager.taskInfo.isCounting
  /**
    * 是否计时
    */
  val isCosting = taskManager.taskInfo.isCosting
  /**
    * 是否记录binlogPosition
    */
  val isProfiling = taskManager.taskInfo.isProfiling
  /**
    * logPosition处理
    */
  val logPositionHandler = taskManager.logPositionHandler
  /**
    * 同步标识作为写入zk 的标识
    */
  val destination = taskManager.taskInfo.syncTaskId
  /**
    * 本次同步任务开始的logPosition
    * 从zk中获取
    */
  val startPosition = Option(logPositionHandler.logPositionManager.getLatestIndexBy(destination))
  /**
    * 待保存的BinlogOffset
    */
  var schedulingSavedOffset: Long = if (startPosition.isDefined) {
    startPosition.get.getPostion.getPosition
  } else 4L
  /**
    * 待保存的Binlog文件名称
    */
  var schedulingSavedJournalName: String = if (startPosition.isDefined) {
    startPosition.get.getPostion.getJournalName
  } else ""
  /**
    * 待保存的BinlogOffset
    */
  var scheduledSavedOffset: Long = schedulingSavedOffset
  /**
    * 待保存的Binlog文件名称
    */
  var scheduledSavedJournalName: String = schedulingSavedJournalName
  /**
    * 待保存的BinlogOffset
    */
  var lastSavedOffset: Long = if (startPosition.isDefined) {
    startPosition.get.getPostion.getPosition
  } else 4L
  /**
    * 待保存的Binlog文件名称
    */
  var lastSavedJournalName: String = if (startPosition.isDefined) {
    startPosition.get.getPostion.getJournalName
  } else ""
  /**
    * 最新时间戳
    */
  var latestTimestamp: Long = 0
  /**
    * 是否出现异常
    */
  var isAbnormal: Boolean = false
  var count = 0

  override def receive: Receive = {
    case SyncControllerMessage(msg) => {
      msg match {

        case "start" => {
          //online模式
          log.info(s"sinker swtich to online,id:$syncTaskId")
          context.become(online)
          sinkerChangeStatus(Status.ONLINE)
        }
        case x => {
          log.warning(s"sinker offline unhandled message:$x,id:$syncTaskId")
        }
      }
    }
  }

  def online: Receive = {
    case kafkaMessage: KafkaMessage => {
      //      if(true)throw new Exception("test")else throw new Exception("test")

      count = count + 1
      val ogIndex = kafkaMessage.getBaseDataJsonKey.syncTaskSequence
      val index: Int = if (ogIndex <= 0) 0 else ogIndex.toInt
      sinkList(index) ! kafkaMessage
    }
    case BinlogPositionInfo(journalName, offset, timestamp) => {
      this.lastSavedJournalName = journalName
      this.lastSavedOffset = offset
      this.latestTimestamp = timestamp
      count = count + 1
      // log.info(s"JournalName update to $savedJournalName,offset update to $savedOffset")
      if (isProfiling) taskManager.sinkerLogPosition.set(s"latest binlog:{$journalName:$offset,timestamp:$latestTimestamp},save point:{$schedulingSavedJournalName:$schedulingSavedOffset},lastSavedPoint:{$scheduledSavedJournalName:$scheduledSavedOffset},id:$syncTaskId")
      if (isCounting) processingCounter.fold(log.error(s"processingCounter cannot be null,id:$syncTaskId"))(ref => ref ! SinkerMessage(1))
    }
    case SyncControllerMessage("save") => {
      if (scheduledSavedJournalName != null && scheduledSavedJournalName.trim != "") {
        logPositionHandler.persistLogPosition(destination, scheduledSavedJournalName, scheduledSavedOffset)
        log.info(s"save logPosition $scheduledSavedJournalName:$scheduledSavedOffset,id:$syncTaskId")
      }
      scheduledSavedJournalName = schedulingSavedJournalName
      scheduledSavedOffset = schedulingSavedOffset
      schedulingSavedOffset = lastSavedOffset
      schedulingSavedJournalName = lastSavedJournalName
    }
    case SinkerMessage(e: Throwable) => {

      sinkerChangeStatus(Status.ERROR)
      context.become(error)
      if (!isAbnormal) {
        isAbnormal = true
        log.error(s"error when sink data,e:$e,message:${e.getMessage},cause:${e.getCause},id:$syncTaskId");
        logPositionHandler.persistLogPosition(destination, scheduledSavedJournalName, scheduledSavedOffset)
        //向上传递
        throw new SinkDataException(s"error when sink data,cause:$e,message:${e.getMessage},id:$syncTaskId", e)
      }
    }
  }

  def error: Receive = {
    case x => log.warning(s"sinker is crashed,cannot handle this message:$x,id:$syncTaskId")
  }

  def initSinkers = {
    //0号作为ddl处理器
    log.info(s"init sinkers,id:$syncTaskId")
    (0 to sinkerNum)
      .map {
        index =>
          context.actorOf(MysqlBinlogInOrderSinker.props(taskManager, index).withDispatcher("akka.sinker-dispatcher"), s"sinker$index")

      }

  }

  /**
    * 错位次数阈值
    */
  override var errorCountThreshold: Int = _
  /**
    * 错位次数
    */
  override var errorCount: Int = _

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???


  /**
    * ********************* 状态变化 *******************
    */

  override def changeFunc(status: Status) = TaskManager.changeFunc(status, taskManager)

  override def onChangeFunc = TaskManager.onChangeStatus(taskManager)

  override def sinkerChangeStatus(status: Status) = TaskManager.changeStatus(status, changeFunc, onChangeFunc)


  /**
    * **************** Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    sinkerChangeStatus(Status.OFFLINE)
    initSinkers
    if (isProfiling) taskManager.sinkerLogPosition.set(s"latest binlog:{[]:[]},save point:{$schedulingSavedJournalName:$schedulingSavedOffset},lastSavedPoint:{$scheduledSavedJournalName:$scheduledSavedOffset},id:$syncTaskId")
    log.info(s"switch sinker to offline,id:$syncTaskId")

  }

  override def postStop(): Unit = {
    log.info(s"sinker processing postStop,id:$syncTaskId")
    if (isAbnormal && !StringUtils.isEmpty(scheduledSavedJournalName)) {
      val theJournalName = this.scheduledSavedJournalName
      val theOffset = this.scheduledSavedOffset
      logPositionHandler.persistLogPosition(destination, theJournalName, theOffset)
      log.info(s"记录binlog $theJournalName:$theOffset,id:$syncTaskId")
      if (isProfiling) taskManager.sinkerLogPosition.set(s"latest binlog:{[]:[]},save point:{$theJournalName:$theOffset},lastSavedPoint:{$scheduledSavedJournalName:$scheduledSavedOffset},id:$syncTaskId")
    }
    //    kafkaSinker.kafkaProducer.close()
    //    sinkTaskPool.environment.shutdown()
    //logPositionHandler.logPositionManage
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"sinker processing preRestart,id:$syncTaskId")
    sinkerChangeStatus(Status.ERROR)
    context.become(receive)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"sinker processing preRestart,id:$syncTaskId")
    sinkerChangeStatus(Status.RESTARTING)
    super.postRestart(reason)
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {

      case e: ZkTimeoutException => {
        sinkerChangeStatus(Status.ERROR)
        log.error(s"can not connect to zookeeper server,id:$syncTaskId")
        Escalate
      }
      case e: Exception => {
        sinkerChangeStatus(Status.ERROR)
        log.error(s"sinker crashed,exception:$e,cause:${e.getCause},processing SupervisorStrategy,id:$syncTaskId")
        Escalate
      }
      case error: Error => {
        sinkerChangeStatus(Status.ERROR)
        log.error(s"sinker crashed,error:$error,cause:${error.getCause},processing SupervisorStrategy,id:$syncTaskId")
        Escalate
      }
      case e => {
        log.error(s"sinker crashed,throwable:$e,cause:${e.getCause},processing SupervisorStrategy,id:$syncTaskId")
        sinkerChangeStatus(Status.ERROR)
        Escalate
      }
    }
  }
}

object MysqlBinlogInOrderSinkerManager {
  def props(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager): Props = Props(new MysqlBinlogInOrderSinkerManager(mysql2KafkaTaskInfoManager))
}