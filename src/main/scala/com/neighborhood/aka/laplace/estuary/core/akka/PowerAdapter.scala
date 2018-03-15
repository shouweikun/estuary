package com.neighborhood.aka.laplace.estuary.core.akka

import akka.actor.{Actor, ActorLogging, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, FetcherMessage, SinkerMessage, SyncControllerMessage}
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
      val value = x.toLong
      val nextFetchTimeWriteIndex = (fetchTimeWriteIndex + 1) % size
      fetchTimeArray(nextFetchTimeWriteIndex) = value
      fetchTimeWriteIndex = nextFetchTimeWriteIndex

    }
    case BatcherMessage(x) => {
      val value = x.toLong
      val nextBatchTimeWriteIndex = (batchTimeWriteIndex + 1) % size
      fetchTimeArray(nextBatchTimeWriteIndex) = value
      fetchTimeWriteIndex = nextBatchTimeWriteIndex

    }
    case SinkerMessage(x) => {
      val value = x.toLong
      val nextSinkTimeWriteIndex = (sinkTimeWriteIndex + 1) % size
      fetchTimeArray(nextSinkTimeWriteIndex) = value
      fetchTimeWriteIndex = nextSinkTimeWriteIndex

    }
    case SyncControllerMessage(x) => {
      if (x.equals("cost")) {
        taskManager.fetchCost.set(fetchTimeArray.fold(0)(_ + _) / size)
        taskManager.batchCost.set(batchTimeArray.fold(0)(_ + _) / size)
        taskManager.batchCost.set(batchTimeArray.fold(0)(_ + _) / size)
      }
    }
    case "controll" => {
      //todo controller 逻辑
    }
  }
}

object PowerAdapter {
  def props(taskManager: TaskManager): Props = Props(new PowerAdapter(taskManager))
}
