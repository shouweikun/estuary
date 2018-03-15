package com.neighborhood.aka.laplace.estuary.core.akka

import akka.actor.{Actor, ActorLogging, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.FetcherMessage
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2018/3/15.
  */
class PowerAdapter(taskManager: TaskManager) extends Actor with ActorLogging {

  val size: Int = 100
  var fetchTimeArray: Array[Long] = new Array[Long](size)
  var batchTimeArray: Array[Long] = new Array[Long](size)
  var sinkTimeArray: Array[Long] = new Array[Long](size)

  var fetchTimeWriteIndex = 0

  var batchTimeWriteIndex = 0

  var sinkTimeWriteIndex = 0


  override def receive: Receive = {
    case FetcherMessage(x) => {

    }
  }
}

object PowerAdapter {
  def props(taskManager: TaskManager):Props = Props(new PowerAdapter(taskManager))
}
