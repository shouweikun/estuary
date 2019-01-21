package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype


import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.SourceDataBatcher
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2019/1/9.
  */
trait SourceDataSpecialBatcherPrototype extends ActorPrototype with SourceDataBatcher {
  /**
    * 事件收集器
    */
  def eventCollector: Option[ActorRef]

  /**
    * sinker 的ActorRef
    */
  def sinker: ActorRef

  /**
    * 任务信息管理器
    */
  def taskManager: TaskManager


  override def preStart(): Unit = {
    log.info(s"init special batcher,id:$syncTaskId")
  }

  override def postStop(): Unit = {

  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"special batcher process preRestart,id:$syncTaskId")
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"special batcher process postRestart,id:$syncTaskId")
    super.postRestart(reason)
  }
}

