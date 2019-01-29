package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.control

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.adapt.MysqlBinlogInOrderPowerAdapter
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderBatcherManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp.MysqlBinlogInOrderBatcherMysqlManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.count.MysqlInOrderProcessingCounter
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch.MysqlBinlogInOrderFetcherManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.listen.MysqlConnectionInOrderListener
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.record.MysqlBinlogInOrderPositionRecorder
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.{MysqlBinlogInOrderMysqlSinkerManager, MysqlBinlogInOrderSinkerManager}
import com.neighborhood.aka.laplace.estuary.mysql.task.{Mysql2MysqlTaskInfoBean, Mysql2MysqlTaskInfoManager}

/**
  * Created by john_liu on 2019/1/15.
  *
  * @note 可动态加载:
  *       ```
  *       $sinkerName
  *       $fetcherame
  *       $batcherName
  *       ```
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInOrderMysqlController(
                                               val totalTaskInfo: Mysql2MysqlTaskInfoBean
                                             ) extends MysqlBinlogInOrderController[MysqlSinkFunc](totalTaskInfo.taskRunningInfoBean, totalTaskInfo.sourceBean, totalTaskInfo.sinkBean) {
  log.info(s"we get totalTaskInfo:${totalTaskInfo},id:$syncTaskId")

  /**
    * 1.初始化HeartBeatsListener
    * 2.初始化binlogSinker
    * 3.初始化binlogEventBatcher
    * 4.初始化binlogFetcher
    * 5.初始化processingCounter
    * 6.初始化powerAdapter
    * 7.初始化positionRecorder
    */
  override final def initWorkers: Unit = {
    log.info(s"start to init all workers,id:$syncTaskId")
    //初始化processingCounter
    log.info(s"initialize processingCounter,id:$syncTaskId")
    val processingCounter = context.actorOf(MysqlInOrderProcessingCounter.props(taskManager), processingCounterName)
    taskManager.processingCounter = Option(processingCounter) //必须要processingCounter嵌入taskManager
    //初始化powerAdapter
    log.info(s"initialize powerAdapter,id:$syncTaskId")
    val powerAdapter = context.actorOf(MysqlBinlogInOrderPowerAdapter.buildMysqlBinlogInOrderPowerAdapterByName(taskManager, ""), powerAdapterName)
    taskManager.powerAdapter = Option(powerAdapter) //必须要powerAdapter嵌入taskManager

    //初始化binlogPositionRecorder
    log.info(s"initialize Recorder,id:$syncTaskId")
    val recorder = context.actorOf(MysqlBinlogInOrderPositionRecorder.props(resourceManager), positionRecorderName)
    taskManager.positionRecorder = Option(recorder) //必须要将PositionRecorder嵌入taskManager


    //初始化HeartBeatsListener
    log.info(s"initialize heartBeatListener,id:$syncTaskId")
    context.actorOf(MysqlConnectionInOrderListener.props(resourceManager).withDispatcher("akka.pinned-dispatcher"), listenerName)

    //初始化binlogSinker
    log.info(s"initialize sinker,id:$syncTaskId")
    val sinkerTypeName = controllerNameToLoad.get(sinkerName).getOrElse(MysqlBinlogInOrderMysqlSinkerManager.name)
    val binlogSinker = context.actorOf(MysqlBinlogInOrderSinkerManager.buildMysqlBinlogInOrderMysqlSinkerManager(resourceManager, sinkerTypeName).withDispatcher("akka.sinker-dispatcher"), sinkerName)

    taskManager.wait4SinkerList() //必须要等待
    //初始化batcher
    log.info(s"initialize batcher,id:$syncTaskId")
    val batcherTypeName = controllerNameToLoad.get(batcherName).getOrElse(MysqlBinlogInOrderBatcherMysqlManager.name)
    val binlogEventBatcher = context
      .actorOf(MysqlBinlogInOrderBatcherManager
        .buildMysqlBinlogInOrderBatcherManager(batcherTypeName, resourceManager, binlogSinker).withDispatcher("akka.batcher-dispatcher"), "binlogBatcher")

    // 初始化binlogFetcher
    log.info(s"initialize fetcher,id:$syncTaskId")
    val fetcherTypeName: String = controllerNameToLoad.get(fetcherName).getOrElse(MysqlBinlogInOrderFetcherManager.name) //暂时没用上
    context.actorOf(MysqlBinlogInOrderFetcherManager.props(resourceManager, binlogEventBatcher).withDispatcher("akka.fetcher-dispatcher"), "binlogFetcher")

  }


  /**
    * 任务资源管理器的构造的工厂方法
    *
    * @return 构造好的资源管理器
    *
    */
  override def buildManager: Mysql2MysqlTaskInfoManager = Mysql2MysqlTaskInfoManager(totalTaskInfo, config)


  /**
    * 资源管理器，一次同步任务所有的resource都由resourceManager负责
    */
  override lazy val resourceManager: Mysql2MysqlTaskInfoManager = super.resourceManager.asInstanceOf[Mysql2MysqlTaskInfoManager]
}

object MysqlBinlogInOrderMysqlController {
  val name = MysqlBinlogInOrderMysqlController.getClass.getName.stripSuffix("$")

  def props(totalTaskInfo: Mysql2MysqlTaskInfoBean): Props = Props(new MysqlBinlogInOrderMysqlController(totalTaskInfo))
}