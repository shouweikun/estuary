package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.control

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.adapt.MysqlBinlogInOrderPowerAdapter
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderBatcherManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp.MysqlBinlogInOrderBatcherMysqlManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.count.MysqlInOrderProcessingCounter
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch.MysqlBinlogInOrderFetcherManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.listen.MysqlConnectionInOrderListener
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.record.MysqlBinlogInOrderPositionRecorder
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.mysql.{MysqlBinlogInOrderMysqlSinkerManager, MysqlBinlogInOrderSinkerManager}
import com.neighborhood.aka.laplace.estuary.mysql.task.mysql.{Mysql2MysqlAllTaskInfoBean, Mysql2MysqlTaskInfoManager}
import com.neighborhood.aka.laplace.estuary.web.service.Mysql2MysqlService

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
final class MysqlBinlogInOrderMysqlController4Sda(
                                                   @volatile var totalTaskInfo: Mysql2MysqlAllTaskInfoBean
                                                 ) extends MysqlBinlogInOrderController[MysqlSinkFunc](totalTaskInfo.taskRunningInfoBean, totalTaskInfo.sourceBean, totalTaskInfo.sinkBean) {
  log.info(s"we get totalTaskInfo:${totalTaskInfo},id:$syncTaskId")


  /**
    * 资源管理器，一次同步任务所有的resource都由resourceManager负责
    */
  override lazy val resourceManager: Mysql2MysqlTaskInfoManager = super.resourceManager.asInstanceOf[Mysql2MysqlTaskInfoManager]

  /**
    * 任务管理器
    */
  override lazy val taskManager: TaskManager = resourceManager

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
        .buildMysqlBinlogInOrderBatcherManager(batcherTypeName, resourceManager, binlogSinker).withDispatcher("akka.batcher-dispatcher"), batcherName)

    // 初始化binlogFetcher
    log.info(s"initialize fetcher,id:$syncTaskId")
    val fetcherTypeName: String = controllerNameToLoad.get(fetcherName).getOrElse(MysqlBinlogInOrderFetcherManager.name) //暂时没用上
    context.actorOf(MysqlBinlogInOrderFetcherManager.props(resourceManager, binlogEventBatcher).withDispatcher("akka.fetcher-dispatcher"), fetcherName)

  }


  /**
    * 任务资源管理器的构造的工厂方法
    *
    * @return 构造好的资源管理器
    *
    */
  override def buildManager: Mysql2MysqlTaskInfoManager = {

    log.info(s"start build manager,current filter pattern is  ${totalTaskInfo.sourceBean.filterPattern},id:$syncTaskId")
    updateBeanInfo
    Mysql2MysqlTaskInfoManager(totalTaskInfo, config)
  }


  /**
    * 更新bean信息
    * 在postRestart时调用
    */
  private def updateBeanInfo: Unit = {
    log.info(s"cause restart happened,try to update bean info,id:$syncTaskId")
    Mysql2MysqlService.ref.getNewTaskInfoBeanAndUpdate(syncTaskId).fold(log.warning(s"update bean info failed,id:$syncTaskId"))(x => totalTaskInfo = x)
    log.info(s"filter pattern after update is ${totalTaskInfo.sourceBean.filterPattern}")
  }


  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    updateBeanInfo //更新bean信息，主要是针对sda信息
  }
}

object MysqlBinlogInOrderMysqlController4Sda {
  lazy val name = MysqlBinlogInOrderMysqlController4Sda.getClass.getName.stripSuffix("$")

  def props(totalTaskInfo: Mysql2MysqlAllTaskInfoBean): Props = Props(new MysqlBinlogInOrderMysqlController4Sda(totalTaskInfo))
}

