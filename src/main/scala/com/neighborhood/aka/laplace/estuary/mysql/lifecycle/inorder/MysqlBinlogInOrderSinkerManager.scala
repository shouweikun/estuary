package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, OneForOneStrategy}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SourceDataSinker, Status}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager
import org.I0Itec.zkclient.exception.ZkTimeoutException
import org.springframework.util.StringUtils

/**
  * Created by john_liu on 2018/5/8.
  */
class MysqlBinlogInOrderSinkerManager(
                                       val mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) extends Actor with SourceDataSinker with ActorLogging {

  val syncTaskId = mysql2KafkaTaskInfoManager.syncTaskId
  val sinkerNum = mysql2KafkaTaskInfoManager.sinkerNum

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
  override def receive: Receive = {

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
    if (isProfiling) mysql2KafkaTaskInfoManager.sinkerLogPosition.set(s"$lastSavedJournalName:$lastSavedOffset")
  }

  override def postStop(): Unit = {
    if (!isAbnormal.get() && !StringUtils.isEmpty(lastSavedJournalName)) {
      val theJournalName = this.scheduledSavedJournalName
      val theOffset = this.scheduledSavedOffset
      logPositionHandler.persistLogPosition(destination, theJournalName, theOffset)
      log.info(s"记录binlog $theJournalName,$theOffset")
      if (isProfiling) mysql2KafkaTaskInfoManager.sinkerLogPosition.set(s"$theJournalName:$theOffset")
    }
    kafkaSinker.kafkaProducer.close()
    sinkTaskPool.environment.shutdown()
    //logPositionHandler.logPositionManage
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    sinkerChangeStatus(Status.RESTARTING)
    context.become(receive)
  }

  override def postRestart(reason: Throwable): Unit = {

    super.postRestart(reason)
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: StackOverflowError => {
        sinkerChangeStatus(Status.ERROR)
        log.error("stackOverFlow")
        Escalate
      }

      case e: ZkTimeoutException => {
        sinkerChangeStatus(Status.ERROR)
        log.error(s"can not connect to zookeeper server,id:$syncTaskId")
        Escalate
      }
      case e: Exception => {
        sinkerChangeStatus(Status.ERROR)
        Escalate
      }
      case error: Error => {
        sinkerChangeStatus(Status.ERROR)
        Escalate
      }
      case _ => {
        sinkerChangeStatus(Status.ERROR)
        Escalate
      }
    }
  }
}
