package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.{HeartBeatListener, Status}
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import com.neighborhood.aka.laplace.estuary.core.task.{RecourceManager, TaskManager}

/**
  * Created by john_liu on 2018/5/20.
  */
trait HeartBeatListenerPrototype[source <: DataSourceConnection] extends ActorPrototype with HeartBeatListener {
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
  protected def changeFunc(status: Status) = TaskManager.changeFunc(status, taskManager)

  protected def onChangeFunc = TaskManager.onChangeStatus(taskManager)

  protected def listenerChangeStatus(status: Status) = TaskManager.changeStatus(status, changeFunc, onChangeFunc)


  /**
    * **************** Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    log.info(s"switch heartBeatListener to offline,id:$syncTaskId")
    listenerChangeStatus(Status.OFFLINE)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"heartBeatListener process preRestart,id:$syncTaskId")
    context.become(receive)
    listenerChangeStatus(Status.RESTARTING)
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"heartBeatListener process postRestart id:$syncTaskId")
    super.postRestart(reason)
  }

  override def postStop(): Unit = {
    log.info(s"heartBeatListener process postStop id:$syncTaskId")
    connection
    connection.disconnect()
  }

}
