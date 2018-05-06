package com.neighborhood.aka.laplace.estuary.mongo.lifecycle

import akka.actor.{Actor, ActorLogging}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.SourceDataBatcher
import com.neighborhood.aka.laplace.estuary.mongo.task.Mongo2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/5/2.
  */
class OplogBatcher(
                    mongo2KafkaTaskInfoManager: Mongo2KafkaTaskInfoManager
                  ) extends SourceDataBatcher with Actor with ActorLogging {
  override def receive: Receive = ???

  lazy val powerAdapter = mongo2KafkaTaskInfoManager.powerAdapter
  lazy val processingCounter = mongo2KafkaTaskInfoManager.processingCounter
  /**
    * 是否记录耗时
    */
  val isCosting: Boolean = mongo2KafkaTaskInfoManager.taskInfoBean.isCosting
  /**
    * 是否计数
    */
  val isCounting: Boolean = mongo2KafkaTaskInfoManager.taskInfoBean.isCounting
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
