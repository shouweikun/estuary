package com.neighborhood.aka.laplace.estuary.mysql.lifecycle

import akka.actor.{Actor, ActorLogging, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle._
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant

/**
  * Created by john_liu on 2018/3/15.
  */
class Mysql2KafkaPowerAdapter(
                               taskManager: TaskManager
                             ) extends Actor with ActorLogging with PowerAdapter {



  override def receive: Receive = {
    /**
      * 记录fetch耗时
      */
    case FetcherMessage(x) => {
      val value = x.toLong
      value match {
        case -1 => val nextFetchTimeWriteIndex = (fetchTimeWriteIndex + 1) % size
          // 如果拿不到数据，默认在时间上随机增加3-5倍
          fetchTimeArray(nextFetchTimeWriteIndex) = (2 * (math.random) + 3).toLong
          fetchTimeWriteIndex = nextFetchTimeWriteIndex
        //          if (System.currentTimeMillis() % 5 == 0) fetchTimeSum += 1
        case _ => {
          val nextFetchTimeWriteIndex = (fetchTimeWriteIndex + 1) % size
          fetchTimeArray(nextFetchTimeWriteIndex) = value
          fetchTimeWriteIndex = nextFetchTimeWriteIndex
          fetchCountSum += 1
          fetchTimeSum += value
        }
      }


    }

    /**
      * 记录batch耗时
      */
    case BatcherMessage(x) => {
      val value = x.toLong
      val nextBatchTimeWriteIndex = (batchTimeWriteIndex + 1) % size
      batchTimeArray(nextBatchTimeWriteIndex) = value
      batchTimeWriteIndex = nextBatchTimeWriteIndex
      batchCountSum += 1
      batchTimeSum += value
    }

    /**
      * 记录sink耗时
      */
    case SinkerMessage(x) => {
      val value = x.toLong
      val nextSinkTimeWriteIndex = (sinkTimeWriteIndex + 1) % size
      sinkTimeArray(nextSinkTimeWriteIndex) = value
      sinkTimeWriteIndex = nextSinkTimeWriteIndex
      sinkCountSum += 1
      sinkTimeSum += value
    }
    case SyncControllerMessage(x) => {
      x match {
        /**
          * 计算各部件耗时并刷新
          */
        case "cost" => {
          computeCost
          //          val fetchCost = (fetchTimeArray.fold(0L)(_ + _))./(size)
          //          val batchCost = (batchTimeArray.fold(0L)(_ + _))./(size)
          //          val sinkCost = (sinkTimeArray.fold(0L)(_ + _))./(size)
          //          taskManager.fetchCost.set(fetchCost)
          //          taskManager.batchCost.set(batchCost)
          //          taskManager.sinkCost.set(sinkCost)
        }

        /**
          * 功率控制
          */
        case "control" => control
      }
    }
  }

  override def control = {
    //todo 好好编写策略
    val sinkCost = taskManager.sinkCost.get()
    val adjustedSinkCost = if (sinkCost <= 0) 1 else sinkCost
    val batchCost = taskManager.batchCost.get()
    val adjustedBatchCost = if (batchCost <= 0) 1 else batchCost
    val fetchCost = taskManager.fetchCost.get()
    val adjustedFetchCost = if (fetchCost <= 0) 1 else fetchCost
    //调节策略
    val batchThreshold = taskManager.batchThreshold.get
    val fetchCount = taskManager.fetchCount.get()
    val batchCount = taskManager.batchCount.get()
    val sinkCount = taskManager.sinkCount.get()
    val batcherNum = taskManager.batcherNum
    val delayDuration = if (sinkCost < batchCost) {
      //sink速度比batch速度快的话
      val left = (adjustedSinkCost * 1000 / batchThreshold - adjustedFetchCost * 1000 + 1) * 70 / 100
      val limitRatio = batcherNum * 3
      val right = 1000 * adjustedBatchCost / limitRatio / batchThreshold
      log.debug(s"adjustedFetchCost:$adjustedFetchCost,adjustedBatchCost:$adjustedBatchCost,adjustedSinkCost:$adjustedSinkCost,left:$left,right:$right,limitRatio:$limitRatio")
      math.max(left, right)
    } else {
      //sink速度比batch速度快的慢
      math.max((adjustedSinkCost * 1000 / batchThreshold - adjustedFetchCost * 1000 + 1) * 70 / 100, 0)
    }
    log.debug(s"delayDuration:$delayDuration")

    val finalDelayDuration: Long = ((fetchCount - sinkCount) / batchThreshold, fetchCost, batchCost, sinkCost) match {
      case (_, x, _, _) if (x > 400) => math.max(150000, delayDuration) ////150ms 休眠
      case (w, _, _, _) if (w < 1 * batcherNum) => 0 //0s 快速拉取数据
      case (_, x, y, z) if (x > 150 || y > 10000 || z > 800) => math.max(100000, delayDuration) //100ms 防止数据太大
      case (_, x, y, z) if (x > 100 || y > 8000 || z > 750) => math.max(80000, delayDuration) //80ms 防止数据太大
      case (_, x, y, z) if (x > 50 || y > 6000 || z > 700) => math.max(60000, delayDuration) //60ms 防止数据太大
      case (_, x, y, z) if (x > 30 || y > 4000 || z > 600) => math.max(40000, delayDuration) //40ms 防止数据太大
      case (_, x, y, z) if (x > 25 || y > 2500 || z > 450) => math.max(35000, delayDuration) //40ms
      case (w, _, _, _) if (w < 8 * batcherNum) => 0 //0s 快速拉取数据
      case (_, x, y, z) if (x > 20 || y > 2000 || z > 400) => math.max(25000, delayDuration) //25ms
      case (_, x, y, z) if (x > 15 || y > 1800 || z > 300) => math.max(20000, delayDuration) //20ms
      case (_, x, y, z) if (x > 12 || y > 1700 || z > 250) => math.max(10000, delayDuration) //10ms
      case (_, x, y, z) if (x > 10 || y > 1300 || z > 200) => math.max(7000, delayDuration) //7ms
      case (_, x, y, z) if (x > 8 || y > 950 || z > 180) => math.max(2000, delayDuration) //2ms
      case (_, x, y, z) if (x > 5 || y > 700 || z > 160) => math.max(1500, delayDuration) //1.5ms
      case (w, _, _, _) if (w < 20 * batcherNum) => delayDuration
      case (w, _, _, _) if (w < 15 * batcherNum) => delayDuration * 15 / 10
      case (w, _, _, _) if (w < 30 * batcherNum) => delayDuration * 7
      case (w, _, _, _) if (w < 70 * batcherNum) => delayDuration * 15
      case (w, _, _, _) if (w < 120 * batcherNum) => math.max(delayDuration * 10, 1000000) //1s
      case (w, _, _, _) if (w < 300 * batcherNum) => math.max(delayDuration * 10, 2000000) //2s
      case _ => math.max(delayDuration, 3000000) //3s

    }
    log.info(s"${(fetchCount - sinkCount) / batchThreshold},$fetchCost,$batchCost,$sinkCost,finalDelayDuration:$finalDelayDuration")
    taskManager.fetchDelay.set(finalDelayDuration)
  }

 override def computeCost = {
    val batchThrehold = taskManager.batchThreshold.get()
    val fetchCost = (fetchTimeArray.fold(0L)(_ + _))./(size)
    val batchCost = (batchTimeArray.fold(0L)(_ + _))./(size)
    val sinkCost = (sinkTimeArray.fold(0L)(_ + _))./(size)
    val fetchCostPercentage = fetchTimeSum / SettingConstant.COMPUTE_COST_CONSTANT / 10
    val batchCostPercentage = batchTimeSum / SettingConstant.COMPUTE_COST_CONSTANT / 10 / taskManager.batcherNum
    val sinkCostPercentage = sinkTimeSum / SettingConstant.COMPUTE_COST_CONSTANT / 10
    val fetchCountPerSecond = fetchCountSum / SettingConstant.COMPUTE_COST_CONSTANT
    val batchCountPerSecond = batchCountSum / SettingConstant.COMPUTE_COST_CONSTANT * batchThrehold
    val sinkCountPerSecond = sinkCountSum / SettingConstant.COMPUTE_COST_CONSTANT * batchThrehold
    taskManager.fetchCost.set(fetchCost)
    taskManager.batchCost.set(batchCost)
    taskManager.sinkCost.set(sinkCost)

    taskManager.fetchCostPercentage.set(fetchCostPercentage)
    taskManager.batchCostPercentage.set(batchCostPercentage)
    taskManager.sinkCostPercentage.set(sinkCostPercentage)

    taskManager.fetchCountPerSecond.set(fetchCountPerSecond)
    taskManager.batchCountPerSecond.set(batchCountPerSecond)
    taskManager.sinkCountPerSecond.set(sinkCountPerSecond)

    fetchCountSum = 0
    fetchTimeSum = 0
    batchTimeSum = 0
    batchCountSum = 0
    sinkTimeSum = 0
    sinkCountSum = 0
  }
}

object Mysql2KafkaPowerAdapter {


  def props(taskManager: TaskManager): Props = Props(new Mysql2KafkaPowerAdapter(taskManager))
}
