package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.SourceDataBatcher
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2018/5/20.
  */
trait SourceDataBatcherManagerPrototype extends ActorPrototype with SourceDataBatcher {
  /**
    * 是否是最上层的manager
     */
  val isHead:Boolean

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
  val syncTaskId = taskManager.syncTaskId
}
