package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.SourceDataBatcher
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, SourceManager, TaskManager}

/**
  * Created by john_liu on 2018/5/20.
  *
  * @tparam A Source
  * @tparam B Sink
  * @author neighborhood.aka.laplace
  */
trait SourceDataBatcherManagerPrototype[A <: DataSourceConnection, B <: SinkFunc] extends ActorPrototype with SourceDataBatcher {
  /**
    * specialInfoSender的名称
    */
  val specialInfoSenderName = "specialInfoSender"
  /**
    * router的名称
    */
  val routerName: String = "router"

  /**
    * 事件收集器
    */
  def eventCollector: Option[ActorRef]

  /**
    * 是否是最上层的manager
    */
  def isHead: Boolean

  /**
    * sinker 的ActorRef
    */
  def sinker: ActorRef

  /**
    * 任务信息管理器
    */
  def taskManager: TaskManager

  /**
    * 数据源资源管理器
    */
  def sourceManager: SourceManager[A] = ???

  /**
    * 数据汇管理器
    */
  def sinkManager: SinkManager[B] = ???

  /**
    * 编号
    */
  def num: Int

  /**
    * 同步任务id
    */
  def syncTaskId: String

  /**
    * 初始化Batchers
    */
  protected def initBatchers: Unit
}
