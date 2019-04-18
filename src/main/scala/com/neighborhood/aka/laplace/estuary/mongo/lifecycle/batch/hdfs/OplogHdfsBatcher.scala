package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.hdfs

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.bean.support.HdfsMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.BatcherMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataBatcherPrototype
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.OplogClassifier
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt.OplogPowerAdapterCommand.OplogPowerAdapterUpdateCost
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.count.OplogProcessingCounterCommand.OplogProcessingCounterUpdateCount
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset

/**
  * Created by john_liu on 2019/3/1.
  *
  * @author neighborhood.aka.laplace
  */
final class OplogHdfsBatcher(
                              override val taskManager: TaskManager,
                              override val sinker: ActorRef,
                              override val num: Int
                            ) extends SourceDataBatcherPrototype[OplogClassifier, HdfsMessage[MongoOffset]] {

  /**
    * mappingFormat
    */
  override val mappingFormat: MappingFormat[OplogClassifier, HdfsMessage[MongoOffset]] = taskManager.batchMappingFormat.get.asInstanceOf[MappingFormat[OplogClassifier, HdfsMessage[MongoOffset]]]
  /**
    * processingCounter
    */
  lazy val processingCounter = taskManager.processingCounter
  /**
    * powerAdapter
    */
  lazy val powerAdapter = taskManager.powerAdapter
  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

  override def receive: Receive = {
    case BatcherMessage(oplogClassifier: OplogClassifier) => handleBatchTask(oplogClassifier)
    case oplogClassifier: OplogClassifier => handleBatchTask(oplogClassifier)
  }

  /**
    * 处理转化打包任务
    *
    * @param oplogClassifier
    */
  private def handleBatchTask(oplogClassifier: OplogClassifier): Unit = {
    val hdfsMessage = transAndSend(oplogClassifier)
    if (!hdfsMessage.isAbnormal) {
      sendCost(System.currentTimeMillis() - oplogClassifier.fetchTimeStamp)
    }
    sendCount(1)
  }

  @inline
  private def transAndSend(oplogClassifier: OplogClassifier): HdfsMessage[MongoOffset] = {
    val hdfsMessage = transform(oplogClassifier)
    sinker ! hdfsMessage
    hdfsMessage
  }

  @inline
  private def sendCost(cost: Long): Unit = powerAdapter.map(ref => ref ! BatcherMessage(OplogPowerAdapterUpdateCost(cost)))

  @inline
  private def sendCount(count: Long): Unit = processingCounter.map(ref => ref ! BatcherMessage(OplogProcessingCounterUpdateCount(count: Long)))

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

object OplogHdfsBatcher {
  def props(taskManager: TaskManager,
            sinker: ActorRef,
            num: Int): Props = Props(new OplogHdfsBatcher(taskManager, sinker, num))
}