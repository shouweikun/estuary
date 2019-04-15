package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.hdfs

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.BatcherMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSpecialBatcherPrototype
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.OplogBatcherCommand.OplogBatcherCheckHeartbeats

/**
  * Created by john_liu on 2019/3/4.
  *
  * @author neighborhood.aka.laplace
  */
final class OplogHdfsSpecialInfoSender(
                                        override val sinker: ActorRef,
                                        override val taskManager: TaskManager
                                      ) extends SourceDataSpecialBatcherPrototype {

  val logIsEnabled = taskManager.logIsEnabled
  /**
    * 事件收集器
    */
  override val eventCollector: Option[ActorRef] = taskManager.eventCollector

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

  val tableName: String = if (syncTaskId.contains("::")) syncTaskId.split("::")(1) else syncTaskId

  override def receive: Receive = {
    case BatcherMessage(OplogBatcherCheckHeartbeats) => buildAndSendHeartbeatMessage
  }

  def buildAndSendHeartbeatMessage: Unit = {
    if (logIsEnabled) log.warning("buildAndSendHeartbeatMessage should be implemented")

    //    val dummyValue: String = ???
    //    val dummyKey: OplogKey = ???
    //
    //    val dummyKafkaMessage: KafkaMessage = KafkaMessage(dummyKey, dummyValue)
    //    sinker ! dummyKafkaMessage
  }

  /**
    * 错位次数阈值
    */
  override def errorCountThreshold: Int = 0

  /**
    * 错位次数
    */
  override var errorCount: Int = 0

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {}
}

object OplogHdfsSpecialInfoSender {
  def props(sinker: ActorRef, taskManager: TaskManager): Props = Props(new OplogHdfsSpecialInfoSender(sinker, taskManager))

}