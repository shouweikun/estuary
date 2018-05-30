package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import akka.actor.{Actor, ActorLogging}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2018/5/20.
  */
trait ActorPrototype extends Actor with ActorLogging {
  /**
    * 任务信息管理器
    */
  val taskManager: TaskManager

  /**
    * 同步任务id
    */
  val syncTaskId:String
}
