package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import com.neighborhood.aka.laplace.estuary.bean.datasink.DataSinkBean
import com.neighborhood.aka.laplace.estuary.bean.exception.control.RestartCommandException
import com.neighborhood.aka.laplace.estuary.bean.identity.BaseExtractBean
import com.neighborhood.aka.laplace.estuary.bean.resource.DataSourceBase
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.{Status, SyncController}
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, SourceManager, TaskManager}
import com.typesafe.config.Config

/**
  * Created by john_liu on 2018/5/21.
  *
  * @tparam A source
  * @tparam B sinkFunc
  * @author neighborhood.aka.laplace
  */
trait SyncControllerPrototype[A <: DataSourceConnection, B <: SinkFunc] extends ActorPrototype with SyncController {

  /**
    * 用于动态传参加载类和获取ActorRef
    */
  final val sinkerName: String = "binlogSinker"
  final val fetcherName: String = "binlogFetcher"
  final val batcherName: String = "binlogBatcher"
  final val listenerName: String = "heartBeatsListener"
  final val powerAdapterName: String = "powerAdapter"
  final val processingCounterName: String = "processingCounter"
  final val positionRecorderName: String = "positionRecorder"

  /**
    *
    */
  def controllerNameToLoad: Map[String, String] = taskManager.controllerNameToLoad

  /**
    * 配置
    */
  def config: Config = this.context.system.settings.config

  /**
    * 任务信息bean
    */
  def taskBean: BaseExtractBean

  /**
    * 数据源信息
    */
  def sourceBean: DataSourceBase[A]

  /**
    * 数据汇信息
    */
  def sinkBean: DataSinkBean[B]

  /**
    * 任务信息管理器
    */
  def taskManager: TaskManager

  /**
    * 在线模式
    *
    * @return
    */
  protected def online: Receive

  /**
    * 任务资源管理器的构造的工厂方法
    *
    * @return 构造好的资源管理器
    *
    */
   def buildManager: SourceManager[A] with SinkManager[B] with TaskManager

  /**
    * 利用监督机制重启
    */
  protected def restartBySupervisor: Unit = throw new RestartCommandException(
    {
      log.warning(s"restart sync task:$syncTaskId at ${System.currentTimeMillis()}")
      s"restart a,id:$syncTaskId"
    }
  )

  /**
    * 检查信息
    * 目前包括
    * 1. fetchCount
    * 2. batchCount
    * 3. sinkCount
    * 4. profile
    */
  protected def checkInfo: Unit = {
    lazy val fetchCount = taskManager.fetchCount.get()
    lazy val batchCount = taskManager.batchCount.get()
    lazy val sinkCount = taskManager.sinkCount.get()
    lazy val fetchCost = taskManager.fetchCost.get()
    lazy val batchCost = taskManager.batchCost.get()
    lazy val sinkCost = taskManager.sinkCost.get()
    lazy val delay = taskManager.fetchDelay.get()
    lazy val profile = taskManager.sinkerLogPosition.get()
    lazy val diff = fetchCount - sinkCount
    lazy val message =
      s"""
        {
         "syncTaskId":"$syncTaskId",
         "delay":$delay,
         "fetchCount":$fetchCount,
         "batchCount":$batchCount,
         "sinkCount":$sinkCount,
         "gap":$diff,
         "fetchCost":$fetchCost,
         "batchCost":$batchCost,
         "sinkCost":$sinkCost
        }
      """.stripMargin
    if (diff > 10000) log.warning(message) else log.info(message)
    log.info(profile)
  }

  /**
    * 切换为开始模式
    * 1. context切换
    * 2. 状态online
    * 3.启动所有worker
    */
  protected def switch2Online: Unit = {
    context.become(online, true)
    controllerChangeStatus(Status.ONLINE)
    startAllWorkers
    log.info(s"controller switched to online,start all workers,id:$syncTaskId")
  }

  /**
    * 初始化workers
    */
  protected def initWorkers: Unit

  /**
    * 启动所有worker
    */
  protected def startAllWorkers: Unit

  /**
    * ********************* 状态变化 *******************
    */
  protected def changeFunc(status: Status) = TaskManager.changeFunc(status, taskManager)

  protected def onChangeFunc = TaskManager.onChangeStatus(taskManager)

  protected def controllerChangeStatus(status: Status) = TaskManager.changeStatus(status, changeFunc, onChangeFunc)
}
