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
      batchTimeArray(nextBatchTimeWriteIndex) = value
      batchTimeWriteIndex = nextBatchTimeWriteIndex

    }
    case SinkerMessage(x) => {
      val value = x.toLong
      val nextSinkTimeWriteIndex = (sinkTimeWriteIndex + 1) % size
      sinkTimeArray(nextSinkTimeWriteIndex) = value
      sinkTimeWriteIndex = nextSinkTimeWriteIndex

    }
    case SyncControllerMessage(x) => {
      if (x.equals("cost")) {
        val fetchCost = (fetchTimeArray.fold(0L)(_ + _))./(size)
        val batchCost = (batchTimeArray.fold(0L)(_ + _))./(size)
        val sinkCost = (sinkTimeArray.fold(0L)(_ + _))./(size)
        taskManager.fetchCost.set(fetchCost)
        taskManager.batchCost.set(batchCost)
        taskManager.sinkCost.set(sinkCost)
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
