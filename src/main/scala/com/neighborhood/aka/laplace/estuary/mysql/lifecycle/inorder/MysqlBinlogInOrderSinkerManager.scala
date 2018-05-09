package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props}
import com.neighborhood.aka.laplace.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SinkerMessage, SourceDataSinker, Status, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager
import org.I0Itec.zkclient.exception.ZkTimeoutException
import org.springframework.util.StringUtils

/**
  * Created by john_liu on 2018/5/8.
  */
class MysqlBinlogInOrderSinkerManager(
                                       val mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) extends Actor with SourceDataSinker with ActorLogging {

  val syncTaskId = mysql2KafkaTaskInfoManager.syncTaskId
  lazy val sinkList = context.children.toList
  val sinkerNum = mysql2KafkaTaskInfoManager.sinkerNum
  lazy val processingCounter = mysql2KafkaTaskInfoManager.processingCounter
  /**
    * 是否计数
    */
  val isCounting = mysql2KafkaTaskInfoManager.taskInfo.isCounting
  /**
    * 是否计时
    */
  val isCosting = mysql2KafkaTaskInfoManager.taskInfo.isCosting
  /**
    * 是否记录binlogPosition
    */
  val isProfiling = mysql2KafkaTaskInfoManager.taskInfo.isProfiling
  /**
    * logPosition处理
    */
  val logPositionHandler = mysql2KafkaTaskInfoManager.logPositionHandler
  /**
    * 同步标识作为写入zk 的标识
    */
  val destination = mysql2KafkaTaskInfoManager.taskInfo.syncTaskId
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
      count = count + 1
      val ogIndex = kafkaMessage.getBaseDataJsonKey.syncTaskSequence
      val index: Int = if (ogIndex <= 0) 0 else ogIndex.toInt
      sinkList(index) ! kafkaMessage
    }
    case BinlogPositionInfo(journalName, offset) => {
      this.lastSavedJournalName = journalName
      this.lastSavedOffset = offset
      count = count + 1
      // log.info(s"JournalName update to $savedJournalName,offset update to $savedOffset")
      if (isProfiling) mysql2KafkaTaskInfoManager.sinkerLogPosition.set(s"latest binlog:{$journalName:$offset},save point:{$schedulingSavedJournalName:$schedulingSavedOffset},lastSavedPoint:{$scheduledSavedJournalName:$scheduledSavedOffset},id:$syncTaskId")
      if (isCounting) processingCounter.fold(log.error(s"processingCounter cannot be null,id:$syncTaskId"))(ref => ref ! SinkerMessage(1))
    }
    case SyncControllerMessage("save") => {
      if (schedulingSavedJournalName != null && schedulingSavedJournalName.trim != "") {
        logPositionHandler.persistLogPosition(destination, schedulingSavedJournalName, schedulingSavedOffset)
        log.info(s"save logPosition $schedulingSavedJournalName:$schedulingSavedOffset,id:$syncTaskId")
      }
      scheduledSavedJournalName = schedulingSavedJournalName
      scheduledSavedOffset = schedulingSavedOffset
      schedulingSavedOffset = lastSavedOffset
      schedulingSavedJournalName = lastSavedJournalName
    }
    case SinkerMessage(e: Throwable) => {
      sinkerChangeStatus(Status.ERROR)
      isAbnormal = true
      log.error(s"error when sink data,cause:$e,message:${e.getMessage},id:$syncTaskId");
      //向上传递
      throw new Exception(s"error when sink data,cause:$e,message:${e.getMessage},id:$syncTaskId")
    }
  }

  def initSinkers = {
    //0号作为ddl处理器
    log.info(s"init sinkers,id:$syncTaskId")
    (0 to sinkerNum)
      .map {
        index =>
          context.actorOf(MysqlBinlogInOrderSinker.props(mysql2KafkaTaskInfoManager, index).withDispatcher("akka.sinker-dispatcher"), s"sinker$index")

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

  private def changeFunc(status: Status) = TaskManager.changeFunc(status, mysql2KafkaTaskInfoManager)

  private def onChangeFunc = TaskManager.onChangeStatus(mysql2KafkaTaskInfoManager)

  private def sinkerChangeStatus(status: Status) = TaskManager.changeStatus(status, changeFunc, onChangeFunc)


  /**
    * **************** Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    sinkerChangeStatus(Status.OFFLINE)
    initSinkers
    if (isProfiling) mysql2KafkaTaskInfoManager.sinkerLogPosition.set(s"$lastSavedJournalName:$lastSavedOffset")
    log.info(s"switch sinker to offline,id:$syncTaskId")

  }

  override def postStop(): Unit = {
    log.info(s"sinker processing postStop,id:$syncTaskId")
    if (isAbnormal && !StringUtils.isEmpty(scheduledSavedJournalName)) {
      val theJournalName = this.scheduledSavedJournalName
      val theOffset = this.scheduledSavedOffset
      logPositionHandler.persistLogPosition(destination, theJournalName, theOffset)
      log.info(s"记录binlog $theJournalName:$theOffset,id:$syncTaskId")
      if (isProfiling) mysql2KafkaTaskInfoManager.sinkerLogPosition.set(s"$theJournalName:$theOffset")
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