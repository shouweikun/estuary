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
        case "cost" => {
          val fetchCost = (fetchTimeArray.fold(0L)(_ + _))./(size)
          val batchCost = (batchTimeArray.fold(0L)(_ + _))./(size)
          val sinkCost = (sinkTimeArray.fold(0L)(_ + _))./(size)
          taskManager.fetchCost.set(fetchCost)
          taskManager.batchCost.set(batchCost)
          taskManager.sinkCost.set(sinkCost)
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

            val left = (adjustedSinkCost * 1000 / batchThreshold - adjustedFetchCost * 1000 + 1) * 8 / 10
            val limitRatio = 6
            val right = 1000 * adjustedBatchCost / limitRatio / batchThreshold
            log.info(s"adjustedFetchCost:$adjustedFetchCost,adjustedBatchCost:$adjustedBatchCost,adjustedSinkCost:$adjustedSinkCost,left:$left,right:$right,limitRatio:$limitRatio")
            math.max(left, right)
          } else {
            //sink速度比batch速度快的慢
            math.max((adjustedSinkCost * 1000 / batchThreshold - adjustedFetchCost * 1000 + 1) * (0.8).toLong, 0)
          }
          log.info(s"delayDuration:$delayDuration")
          val sinkCount = taskManager.sinkCount.get()
          val batchCount = taskManager.batchCount.get()
          val fetchCount = taskManager.fetchCount.get()
          val finalDelayDuration: Long = ((fetchCount - sinkCount) / batchThreshold, fetchCost, batchCost, sinkCost) match {
            case (_, x, y, z) if (x > 20 || y > 500 || z > 300) => math.max(50000, delayDuration)//50ms
            case (w, _, _, _) if (w < 5) => 0
            case (w, _, _, _) if (w < 50) => delayDuration
            case (w, _, _, _) if (w < 200) => delayDuration * 15 / 10
            case (w, _, _, _) if (w < 1000) => delayDuration * 7
            case (w, _, _, _) if (w < 2000) => delayDuration * 10
            case _ => math.max(delayDuration,60000000) //一分钟

          }
          log.info(s"finalDelayDuration:$finalDelayDuration")
          taskManager.fetchDelay.set(finalDelayDuration)
        }
      }
    }
  }
}

object PowerAdapter {
  def props(taskManager: TaskManager): Props = Props(new PowerAdapter(taskManager))
}
