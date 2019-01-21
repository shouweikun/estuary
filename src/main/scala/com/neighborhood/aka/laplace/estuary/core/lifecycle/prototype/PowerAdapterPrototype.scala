package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.PowerAdapter
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2018/5/21.
  */
trait PowerAdapterPrototype extends ActorPrototype with PowerAdapter{
  /**
    * 任务信息管理器
    */
  val taskManager: TaskManager

  /**
    * 同步任务id
    */
  override val syncTaskId:String
}
