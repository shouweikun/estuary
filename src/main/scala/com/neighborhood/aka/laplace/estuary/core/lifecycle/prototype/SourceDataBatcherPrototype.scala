package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.SourceDataBatcher
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat

/**
  * Created by john_liu on 2018/5/20.
  */
trait SourceDataBatcherPrototype[A, B] extends ActorPrototype with SourceDataBatcher with MappingFormat[A, B] {
  /**
    * sinker 的ActorRef
    */
  val sinker: ActorRef
  /**
    * 任务信息管理器
    */
  val taskManager: TaskManager
  /**
    * 编号
    */
  val num:Int
  /**
    * 同步任务id
    */
  override val syncTaskId = taskManager.syncTaskId



  override def preStart(): Unit = {
    log.info(s"init batcher$num,id:$syncTaskId")
  }

  override def postStop(): Unit = {

  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"batcher$num process preRestart,id:$syncTaskId")
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"batcher$num process postRestart,id:$syncTaskId")
    super.postRestart(reason)
  }
}
