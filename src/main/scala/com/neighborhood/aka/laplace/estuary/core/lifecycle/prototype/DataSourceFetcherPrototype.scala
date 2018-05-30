package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.{SourceDataFetcher, Status}
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import com.neighborhood.aka.laplace.estuary.core.task.{RecourceManager, TaskManager}

/**
  * Created by john_liu on 2018/5/30.
  */
trait DataSourceFetcherPrototype[source <: DataSourceConnection] extends ActorPrototype with SourceDataFetcher {
  /**
    * 任务信息管理器
    */
  val taskManager: TaskManager
  /**
    * 资源管理器
    */
  val recourceManager: RecourceManager[_, source, _]
  /**
    * 同步任务id
    */
  override val syncTaskId = taskManager.syncTaskId
  /**
    * 数据源链接
    */
  lazy val connection = recourceManager.source.fork

  /**
    * ********************* 状态变化 *******************
    */
  protected def changeFunc(status: Status): Unit = TaskManager.changeFunc(status, taskManager)

  protected def onChangeFunc: Unit = TaskManager.onChangeStatus(taskManager)

  protected def fetcherChangeStatus(status: Status): Unit = TaskManager.changeStatus(status, changeFunc, onChangeFunc)

  /**
    * ********************* Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    log.info(s"fetcher switch to offline,id:$syncTaskId")
    if (connection.isConnected) connection.disconnect()
    //状态置为offline
    fetcherChangeStatus(Status.OFFLINE)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"fetcher processing postRestart,id:$syncTaskId")
    super.postRestart(reason)

  }

  override def postStop(): Unit = {
    log.info(s"fetcher processing postStop,id:$syncTaskId")
    connection.disconnect()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"fetcher processing preRestart,id:$syncTaskId")
    context.become(receive)
    fetcherChangeStatus(Status.RESTARTING)
    super.preRestart(reason, message)
  }


}
