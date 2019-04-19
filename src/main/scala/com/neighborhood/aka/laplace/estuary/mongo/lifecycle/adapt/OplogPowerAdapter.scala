package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt

import akka.actor.{Actor, ActorLogging, Props}
import com.neighborhood.aka.laplace.estuary.bean.exception.power.GapTooLargeException
import com.neighborhood.aka.laplace.estuary.core.lifecycle._
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.PowerAdapter
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import OplogPowerAdapterCommand._

/**
  * Created by john_liu on 2018/5/5.
  * 功率控制器
  * 用于计算功率参数并调节fetcher速度
  *
  * @note 注意，实现新的功率控制器需要在工厂方法更新
  * @since 2019-01-09
  * @author neighborhood.aka.lapalce
  */
abstract class OplogPowerAdapter(
                                  taskManager: TaskManager
                                ) extends Actor with ActorLogging with PowerAdapter {

  val max: Long = taskManager.maxMessageGap
  val syncTaskId = taskManager.syncTaskId
  val batchNum = taskManager.batcherNum
  val sinkNum = taskManager.sinkerNum // 和batcher数量相等
  var increaseTend = 0 //判断增加趋势的
  var lastDiff: Long = 0
  var lastDelayDuration: Long = 0

  override def receive: Receive = {
    case FetcherMessage(OplogPowerAdapterUpdateCost(cost)) => updateFetchTimeByTimeCost(cost)
    case BatcherMessage(OplogPowerAdapterUpdateCost(cost)) => updateBatchTimeByTimeCost(cost)
    case SinkerMessage(OplogPowerAdapterUpdateCost(cost)) => updateSinkTimeByTimeCost(cost)
    case SyncControllerMessage(OplogPowerAdapterControl)
    => control
    case SyncControllerMessage(OplogPowerAdapterComputeCost)
    => computeCost
  }

  override def computeCost: Unit = {
    //百分比
    taskManager.fetchCostPercentage.set(computeCostPercentage(fetchTimeSum, SettingConstant.COMPUTE_COST_CONSTANT))
    taskManager.batchCostPercentage.set(computeCostPercentage(batchTimeSum, SettingConstant.COMPUTE_COST_CONSTANT))
    taskManager.sinkCostPercentage.set(computeCostPercentage(sinkTimeSum, SettingConstant.COMPUTE_COST_CONSTANT))


    //实时耗时
    computeActualCost

    taskManager.fetchCost.set(fetchActualTimeCost)
    taskManager.batchCost.set(batchActualTimeCost)
    taskManager.sinkCost.set(sinkActualTimeCost)

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
    lazy val diff = (fetchCount - sinkCount)
    //这块主要是判断增长趋势的
    if (diff > lastDiff) {
      increaseTend += 1
      if (increaseTend > 100000) log.warning(s"gap:$diff which is too large,increaseTrend:$increaseTend>100,id:$syncTaskId")
      if (increaseTend > 300 && diff > 50000) throw new GapTooLargeException(s"gap:$diff which is too large,increaseTrend:$increaseTend>300,id:$syncTaskId")
    } else increaseTend = 0
    lastDiff = diff
    val computeDelayDuration: Long = (diff, fetchCost, batchCost, sinkCost) match {
      //      case _ => 10000  //做实验用
      //      case (_, x, _, _) if (x > 500000) => math.max(3000000, delayDuration) //3s 休眠
      //      case (_, x, _, _) if (x > 100000) => math.max(2000000, delayDuration) //2s 休眠
            case (_, _, y, _) if (y> 5000) => math.max(10000, delayDuration) //10ms 休眠
      case (_, x, _, _) if (x > 400) => math.max(200000, delayDuration) ////200ms 休眠
      //      case (w, _, _, _) if (w < 1000 * 1) => 0 //0s 快速拉取数据
      //      case (_, x, y, z) if (x > 150 || y > 10000 || z > 800) => math.max(100000, delayDuration) //100ms 防止数据太大
      //      case (_, x, y, z) if (x > 100 || y > 8000 || z > 750) => math.max(80000, delayDuration) //80ms 防止数据太大
      //      case (_, x, y, z) if (x > 50 || y > 6000 || z > 700) => math.max(60000, delayDuration) //60ms 防止数据太大
      //      case (_, x, y, z) if (x > 30 || y > 4000 || z > 600) => math.max(40000, delayDuration) //40ms 防止数据太大
      //      case (_, x, y, z) if (x > 25 || y > 2500 || z > 450) => math.max(35000, delayDuration) //40ms
      case (w, _, _, _) if (w < 20000 * 1) => 0 //0s 快速拉取数据
      //      case (_, x, y, z) if (x > 20 || y > 2000 || z > 400) => math.max(25000, delayDuration) //25ms
      //      case (_, x, y, z) if (x > 15 || y > 1800 || z > 300) => math.max(20000, delayDuration) //20ms
      //      case (_, x, y, z) if (x > 12 || y > 1700 || z > 250) => math.max(10000, delayDuration) //10ms
      //      case (_, x, y, z) if (x > 10 || y > 1300 || z > 200) => math.max(7000, delayDuration) //7ms
      //      case (_, x, y, z) if (x > 8 || y > 950 || z > 180) => math.max(2000, delayDuration) //2ms
      //      case (_, x, y, z) if (x > 5 || y > 700 || z > 160) => math.max(1500, delayDuration) //1.5ms
      //            case (w, _, _, _) if (w < 20000 * 1) => 1
      case (w, _, _, _) if (w < 45000 * 1) => 2
      //      case (w, _, _, _) if (w < 55000 * 1) => 10
      //      case (w, _, _, _) if (w < 70000 * 1) => 1500
      //      case (w, _, _, _) if (w < 65000 * 1) => 300
      case (w, _, _, _) if (w < 72000 * 1) => 20 //0.05ms
      //      case (w, _, _, _) if (w < 65000 * 1) => 500000 // 5s
      case (w, _, _, _) if (w < 82000 * 1) => math.min(1500000, math.max(delayDuration * 10, 10000)) //10ms
      //      case (w, _, _, _) if (w < 100000 * 1) => math.max(delayDuration, 7000) //7ms
      case (w, _, _, _) if (w < 120000 * 1) => math.min(1500000, math.max(delayDuration, 100000)) //30ms
      //      case (w, _, _, _) if (w < 150000 * 1) => math.max(delayDuration, 500000) //500ms
      //      case (w, _, _, _) if (w < 160000 * 1) => math.max(delayDuration, 700000) //700ms
      //      case (w, _, _, _) if (w < 180000 * 1) => math.max(delayDuration, 800000) //800ms
      //      case (w, _, _, _) if (w < 200000 * 1) => math.max(delayDuration, 1000000) //1000ms
      //      case (w, _, _, _) if (w < 210000 * 1) => math.max(delayDuration, 1500000) //1500ms
      //      case (w, _, _, _) if (w < 240000 * 1) => math.max(delayDuration, 200000) //2000ms
      //      case (w, _, _, _) if (w < 280000 * 1) => math.max(delayDuration, 500000) //2000ms
      case (w, _, _, _) if (w < max * 1) => math.max(delayDuration, 1500000) //15s
      case (w, _, _, _) => throw new GapTooLargeException(s"gap:$w which is too large,id:$syncTaskId")
    }

    val finalDelayDuration = if (increaseTend > 50) computeDelayDuration * 2 else computeDelayDuration

    //    if (finalDelayDuration > 1000000) log.warning(s"warning,gap Is big：${
    //      (diff)
    //    },id:$syncTaskId")
    if (lastDelayDuration != finalDelayDuration) { //如果跟上次的值不同，再做操作
      context.parent ! PowerAdapterMessage(OplogPowerAdapterDelayFetch(finalDelayDuration)) //向syncController发送fetchDelay
      taskManager.fetchDelay.set(finalDelayDuration)
      //      if(math.abs(lastDelayDuration-finalDelayDuration)>1000)log.info(s"${(fetchCount - sinkCount)},$fetchCost,$batchCost,$sinkCost,last:$lastDelayDuration,finalDelayDuration:$finalDelayDuration,id:$syncTaskId")
      lastDelayDuration = finalDelayDuration
      //    log.info(s"$finalDelayDuration")
    }
  }
}

object OplogPowerAdapter {
  def buildOplogPowerAdapterByName(taskManager: TaskManager, name: String): Props = name match {
    case _ => DefaultOplogPowerAdapter.props(taskManager)
  }
}
