package com.neighborhood.aka.laplace.estuary.mongo.lifecycle

import akka.actor.{Actor, ActorLogging}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.PowerAdapter
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2018/5/5.
  */
class Oplog2KafkaPowerAdapter (
                              taskManager: TaskManager
                              ) extends Actor with ActorLogging with PowerAdapter{
  override def receive: Receive = {
    case
  }

  override def computeCost: Unit = ???

  override def control: Unit = ???
}
