package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.BatcherMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataBatcherPrototype
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.OplogClassifier

/**
  * Created by john_liu on 2019/3/1.
  */
final class OplogKafkaBatcher(override val taskManager: TaskManager,
                              override val sinker: ActorRef,
                              override val num: Int
                             ) extends SourceDataBatcherPrototype[OplogClassifier, KafkaMessage] {
  override def mappingFormat: MappingFormat[OplogClassifier, KafkaMessage] = taskManager.batchMappingFormat.get.asInstanceOf[MappingFormat[OplogClassifier, KafkaMessage]]


  /**
    * 同步任务id
    */
  override def syncTaskId: String = taskManager.syncTaskId

  override def receive: Receive = {
    case BatcherMessage(oplogClassifier: OplogClassifier) => transAndSend(oplogClassifier)
    case oplogClassifier: OplogClassifier => transAndSend(oplogClassifier)
  }

  private def transAndSend(oplogClassifier: OplogClassifier): Unit = {
    sinker ! transform(oplogClassifier)
  }

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
