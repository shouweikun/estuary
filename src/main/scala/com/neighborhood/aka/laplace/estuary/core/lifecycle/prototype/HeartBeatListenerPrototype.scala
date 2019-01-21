package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.{HeartBeatListener, Status}
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import com.neighborhood.aka.laplace.estuary.core.task.{SourceManager, TaskManager}

/**
  * Created by john_liu on 2018/5/20.
  */
trait HeartBeatListenerPrototype[source <: DataSourceConnection] extends ActorPrototype with HeartBeatListener {
  /**
    * 任务信息管理器
    */
  def taskManager: TaskManager

  /**
    * 资源管理器
    */
  def sourceManager: SourceManager[source]

  /**
    * 同步任务id
    */
  def syncTaskId: String

  //onlineState
  protected def onlineState: Receive

  protected def stop: Unit = {
    context.become(receive)
    listenerChangeStatus(Status.OFFLINE)
  }

  protected def start: Unit = {
    //变为online状态
    log.info(s"heartBeatListener switch to online,id:$syncTaskId")
    context.become(onlineState)
    Option(connection).fold {
      log.error(s"heartBeatListener connection cannot be null,id:$syncTaskId");
      throw new Exception(s"listener connection cannot be null,id:$syncTaskId")
    }(conn => conn.connect())
    listenerChangeStatus(Status.ONLINE)
  }

  /**
    * 数据源链接
    */
  protected lazy val connection = sourceManager.source.fork


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
