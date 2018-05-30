package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.SourceDataSinker
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{RecourceManager, TaskManager}

/**
  * Created by john_liu on 2018/5/21.
  */
trait SourceDataSinkerPrototype[Sink <: SinkFunc] extends ActorPrototype with SourceDataSinker {
  /**
    * 任务信息管理器
    */
  val taskManager: TaskManager
  /**
    * 资源管理器
    */
  val resourceManger: RecourceManager[_, _, Sink]
  /**
    * sink
    */
  lazy val sink = resourceManger.sink
  /**
    * 同步任务id
    */
  override val syncTaskId = taskManager.syncTaskId
}


