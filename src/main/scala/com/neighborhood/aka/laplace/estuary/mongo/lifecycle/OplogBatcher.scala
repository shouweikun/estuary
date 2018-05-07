package com.neighborhood.aka.laplace.estuary.mongo.lifecycle

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, SourceDataBatcher}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.OplogBatcherManager.Classifer
import com.neighborhood.aka.laplace.estuary.mongo.task.Mongo2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/5/2.
  */
class OplogBatcher(
                    val mongo2KafkaTaskInfoManager: Mongo2KafkaTaskInfoManager,
                    val num: Int,
                    val sinker: ActorRef
                  ) extends SourceDataBatcher with Actor with ActorLogging {


  lazy val powerAdapter = mongo2KafkaTaskInfoManager.powerAdapter
  lazy val processingCounter = mongo2KafkaTaskInfoManager.processingCounter
  val syncTaskId = mongo2KafkaTaskInfoManager.syncTaskId
  /**
    * 是否记录耗时
    */
  val isCosting: Boolean = mongo2KafkaTaskInfoManager.taskInfoBean.isCosting
  /**
    * 是否计数
    */
  val isCounting: Boolean = mongo2KafkaTaskInfoManager.taskInfoBean.isCounting


  override def receive: Receive = {
    //todo
    case Classifer(dbObject) => {
      val before = System.currentTimeMillis()


      //性能分析
      if (isCounting) processingCounter.fold(log.warning(s"cannot find processingCounter,id:$syncTaskId"))(ref => ref ! BatcherMessage(1))
      if (isCosting) powerAdapter.fold(log.warning(s"cannot find powerAdapter,id:$syncTaskId"))(ref => ref ! BatcherMessage(System.currentTimeMillis() - before))
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
}

object OplogBatcher {
  def prop(
            mongo2KafkaTaskInfoManager: Mongo2KafkaTaskInfoManager,
            num: Int,
            sinker: ActorRef
          ): Props = Props(new OplogBatcher(mongo2KafkaTaskInfoManager, num, sinker
  ))
}