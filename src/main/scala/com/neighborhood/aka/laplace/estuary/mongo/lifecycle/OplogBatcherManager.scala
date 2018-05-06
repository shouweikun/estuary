package com.neighborhood.aka.laplace.estuary.mongo.lifecycle

import akka.actor.{Actor, ActorLogging}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.SourceDataBatcher
import com.neighborhood.aka.laplace.estuary.mongo.task.Mongo2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/5/2.
  */
class OplogBatcherManager(
                           mongo2KafkaTaskInfoManager: Mongo2KafkaTaskInfoManager
                         )
  extends SourceDataBatcher with Actor with ActorLogging {


  val syncTaskId: String = mongo2KafkaTaskInfoManager.syncTaskId

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
}
