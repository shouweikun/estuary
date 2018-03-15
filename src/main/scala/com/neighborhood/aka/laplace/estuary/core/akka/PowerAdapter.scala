package com.neighborhood.aka.laplace.estuary.core.akka

import akka.actor.{Actor, ActorLogging, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, FetcherMessage, SinkerMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

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
      x match {
        case "cost" => context.system.scheduler.scheduleOnce(3 seconds, self, "cost")
        case "control" => context.system.scheduler.scheduleOnce(1 minute, self, "control")
      }
    }
    case "cost" => {
      val fetchCost = (fetchTimeArray.fold(0L)(_ + _))./(size)
      val batchCost = (batchTimeArray.fold(0L)(_ + _))./(size)
      val sinkCost = (sinkTimeArray.fold(0L)(_ + _))./(size)
      taskManager.fetchCost.set(fetchCost)
      taskManager.batchCost.set(batchCost)
      taskManager.sinkCost.set(sinkCost)
      context.system.scheduler.scheduleOnce(3 seconds, self, "cost")
    }
    case "control" => {
      //todo 好好编写策略
      val sinkCost = taskManager.sinkCost.get()
      val adjustedSinkCost = if (sinkCost <= 0) 1 else sinkCost
      val batchCost = taskManager.batchCost.get()
      val adjustedBatchCost = if (batchCost <= 0) 1 else batchCost
      val fetchCost = taskManager.fetchCost.get()
      val adjustedFetchCost = if (fetchCost <= 0) 1 else fetchCost
      //调节策略
      val batchThreshold = taskManager.batchThreshold.get
      val delayDuration = if (sinkCost < batchCost) {
        //sink速度比batch速度快的话

        val left = adjustedSinkCost * 1000 / batchThreshold - adjustedFetchCost * 1000 + 1
        val limitRatio = 4
        val right = 1000 * adjustedBatchCost / limitRatio
        math.max(left, right)
      } else {
        //sink速度比batch速度快的慢
        adjustedSinkCost * 1000 / batchThreshold - adjustedFetchCost * 1000 + 1
      }
      taskManager.fetchDelay.set(delayDuration)
      context.system.scheduler.scheduleOnce(1 minute, self, "control")
    }
  }
}

object PowerAdapter {
  def props(taskManager: TaskManager): Props = Props(new PowerAdapter(taskManager))
}
