package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.BatcherMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataBatcherPrototype
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.OplogClassifier
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt.OplogPowerAdapterCommand.OplogPowerAdapterUpdateCost
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.count.OplogProcessingCounterCommand.OplogProcessingCounterUpdateCount

/**
  * Created by john_liu on 2019/3/1.
  *
  * @author neighborhood.aka.laplace
  */
final class OplogKafkaBatcher(override val taskManager: TaskManager,
                              override val sinker: ActorRef,
                              override val num: Int
                             ) extends SourceDataBatcherPrototype[OplogClassifier, KafkaMessage] {
  override val mappingFormat: MappingFormat[OplogClassifier, KafkaMessage] = taskManager.batchMappingFormat.get.asInstanceOf[MappingFormat[OplogClassifier, KafkaMessage]]

  val processingCounter = taskManager.processingCounter
  val powerAdapter = taskManager.powerAdapter
  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

  override def receive: Receive = {
    case BatcherMessage(oplogClassifier: OplogClassifier) => handleBatchTask(oplogClassifier)
    case oplogClassifier: OplogClassifier => transAndSend(oplogClassifier)
  }

  private def handleBatchTask(oplogClassifier: OplogClassifier): Unit = {
    val kafkaMessage = transAndSend(oplogClassifier)
    if (!kafkaMessage.isAbnormal) {
      sendCost(kafkaMessage.baseDataJsonKey.msgSyncUsedTime)
      sendCount(1)
    }

  }

  @inline
  private def transAndSend(oplogClassifier: OplogClassifier): KafkaMessage = {
    val kafkaMessage = transform(oplogClassifier)
    sinker ! kafkaMessage
    kafkaMessage
  }

  private def sendCost(cost: Long): Unit = powerAdapter.map(ref => ref ! BatcherMessage(OplogPowerAdapterUpdateCost(cost)))

  private def sendCount(count: Long): Unit = processingCounter.map(ref => ref ! BatcherMessage(OplogProcessingCounterUpdateCount(count: Long)))

  /**
    * 错位次数阈值
    */
  override def errorCountThreshold: Int = ???

  /**
    * 错位次数
    */
  override var errorCount: Int = _

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???
}

object OplogKafkaBatcher {
  val name = OplogKafkaBatcher.getClass.getName.stripSuffix("$")

  def props(taskManager: TaskManager, sinker: ActorRef, num: Int): Props = Props(new OplogKafkaBatcher(taskManager, sinker, num))
}