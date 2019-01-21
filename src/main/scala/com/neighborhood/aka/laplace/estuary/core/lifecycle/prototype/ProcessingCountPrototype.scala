package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import akka.actor.{Actor, ActorLogging}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.ProcessingCounter

/**
  * Created by john_liu on 2019/1/13.
  */
trait ProcessingCountPrototype extends ProcessingCounter with Actor with ActorLogging{

  /**
    * 同步任务标签
    */
  def syncTaskId:String
}
