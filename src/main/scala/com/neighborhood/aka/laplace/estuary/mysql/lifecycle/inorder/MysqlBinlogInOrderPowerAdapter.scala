package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.{Actor, ActorLogging, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.PowerAdapter
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SyncControllerMessage, _}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager

import scala.util.Try

/**
  * Created by john_liu on 2018/5/5.
  */
class MysqlBinlogInOrderPowerAdapter(
                                      taskManager: Mysql2KafkaTaskInfoManager
                                    ) extends Actor with ActorLogging with PowerAdapter {

  val syncTaskId = taskManager.syncTaskId
  val batchNum = taskManager.batcherNum
  val sinkNum = taskManager.batcherNum // 和batcher数量相等

  override def receive: Receive = {

    case FetcherMessage(x) => {
      x match {
        case timeCost: Long => updateFetchTimeByTimeCost(timeCost)
        case timeCost: Int => updateFetchTimeByTimeCost(timeCost)
        case timeCount: String => lazy val cost = Try(timeCount.toInt).getOrElse(0); fetchCountSum + cost
      }
    }
    case BatcherMessage(x) => {
      x match {
        case timeCost: Long => updateBatchTimeByTimeCost(timeCost)
        case timeCost: Int => updateBatchTimeByTimeCost(timeCost)
      }
    }
    case SinkerMessage(x) => {
      x match {
        case timeCost: Long => updateSinkTimeByTimeCost(timeCost)
        case timeCost: Int => updateSinkTimeByTimeCost(timeCost)
      }
    }
    case SyncControllerMessage("control") => control
    case SyncControllerMessage("cost") => computeCost
  }

  override def computeCost: Unit = {
    //百分比
    taskManager.fetchCostPercentage.set(computeCostPercentage(fetchTimeSum, SettingConstant.COMPUTE_COST_CONSTANT))
    taskManager.batchCostPercentage.set(computeCostPercentage(batchTimeSum, SettingConstant.COMPUTE_COST_CONSTANT))
    taskManager.sinkCostPercentage.set(computeCostPercentage(sinkTimeSum, SettingConstant.COMPUTE_COST_CONSTANT))
    //每秒数量
    taskManager.fetchCountPerSecond.set(computeQuantityPerSecond(fetchCountSum, SettingConstant.COMPUTE_COST_CONSTANT))

    taskManager.batchCountPerSecond.set(computeQuantityPerSecond(batchCountSum, SettingConstant.COMPUTE_COST_CONSTANT))

    taskManager.sinkCountPerSecond.set(computeQuantityPerSecond(sinkCountSum, SettingConstant.COMPUTE_COST_CONSTANT))

    //实时耗时
    computeActualCost

    taskManager.fetchCost.set(fetchActualTimeCost)
    taskManager.batchCost.set(batchActualTimeCost)
    taskManager.sinkCost.set(sinkActualTimeCost)

    fetchCountSum = 0
    batchCountSum = 0
    sinkCountSum = 0
    fetchTimeSum = 0
    batchTimeSum = 0
    sinkTimeSum = 0
  }

  override def control: Unit = {
    //实时耗时
    computeActualCost
    val sinkCost = this.sinkActualTimeCost
    val adjustedSinkCost = if (sinkCost <= 0) 1 else sinkCost
    val batchCost = this.batchActualTimeCost
    val adjustedBatchCost = if (batchCost <= 0) 1 else batchCost
    val fetchCost = this.fetchActualTimeCost
    val adjustedFetchCost = if (fetchCost <= 0) 1 else fetchCost
    //调节策略
    val fetchCount = taskManager.fetchCount.get()
    val batchCount = taskManager.batchCount.get()
    val sinkCount = taskManager.sinkCount.get()
    val batcherNum = taskManager.batcherNum
    val delayDuration = if (sinkCost < batchCost) {
      //sink速度比batch速度快的话
      val left = (adjustedSinkCost * 1000 - adjustedFetchCost * 1000 + 1) * 70 / 100
      val limitRatio = batcherNum * 3
      val right = 1000 * adjustedBatchCost / limitRatio
      log.debug(s"adjustedFetchCost:$adjustedFetchCost,adjustedBatchCost:$adjustedBatchCost,adjustedSinkCost:$adjustedSinkCost,left:$left,right:$right,limitRatio:$limitRatio,id:$syncTaskId")
      math.max(left, right)
    } else {
      //sink速度比batch速度快的慢
      math.max((adjustedSinkCost * 1000 - adjustedFetchCost * 1000 + 1) * 70 / 100, 0)
    }
    log.debug(s"delayDuration:$delayDuration,id:$syncTaskId")

    val finalDelayDuration: Long = ((fetchCount - sinkCount), fetchCost, batchCost, sinkCost) match {
      //      case _ => 10000  //做实验用
      case (_, x, _, _) if (x > 2000) => math.max(3000000, delayDuration) //3s 休眠
      case (_, x, _, _) if (x > 1500) => math.max(2000000, delayDuration) //2s 休眠
      case (_, x, _, _) if (x > 1000) => math.max(1000000, delayDuration) //1s 休眠
      case (_, x, _, _) if (x > 400) => math.max(200000, delayDuration) ////200ms 休眠
      case (w, _, _, _) if (w < 3 * batcherNum) => 0 //0s 快速拉取数据
      case (_, x, y, z) if (x > 150 || y > 10000 || z > 800) => math.max(100000, delayDuration) //100ms 防止数据太大
      case (_, x, y, z) if (x > 100 || y > 8000 || z > 750) => math.max(80000, delayDuration) //80ms 防止数据太大
      case (_, x, y, z) if (x > 50 || y > 6000 || z > 700) => math.max(60000, delayDuration) //60ms 防止数据太大
      case (_, x, y, z) if (x > 30 || y > 4000 || z > 600) => math.max(40000, delayDuration) //40ms 防止数据太大
      case (_, x, y, z) if (x > 25 || y > 2500 || z > 450) => math.max(35000, delayDuration) //40ms
      case (w, _, _, _) if (w < 5 * batcherNum) => 0 //0s 快速拉取数据
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
    log.info(s"${(fetchCount - sinkCount)},$fetchCost,$batchCost,$sinkCost,finalDelayDuration:$finalDelayDuration,id:$syncTaskId")
    context.parent ! SyncControllerMessage(finalDelayDuration)
    taskManager.fetchDelay.set(finalDelayDuration)

  }
}

object MysqlBinlogInOrderPowerAdapter {
  def props(taskManager: Mysql2KafkaTaskInfoManager): Props = Props(new MysqlBinlogInOrderPowerAdapter(taskManager))
}
