package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{ActorRef, AllForOneStrategy}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.{SourceDataFetcher, Status}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2018/5/20.
  */
trait SourceDataFetcherManagerPrototype extends ActorPrototype with SourceDataFetcher {
  val directFetcherName: String = "directFetcher"

  /**
    * 是否是最上层的manager
    */
  def isHead: Boolean

  /**
    * batcher 的ActorRef
    *
    */
  val batcher: ActorRef

  /**
    * 任务信息管理器
    */
  def taskManager: TaskManager

  /**
    * 直接fetcher
    */
  def directFetcher: Option[ActorRef] = context.child(directFetcherName)

  /**
    * 初始化Fetcher域下相关组件
    */
  protected def initFetchers: Unit

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
    //状态置为offline
    fetcherChangeStatus(Status.OFFLINE)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"fetcher processing postRestart,id:$syncTaskId")
    super.postRestart(reason)

  }

  override def postStop(): Unit = {
    log.info(s"fetcher processing postStop,id:$syncTaskId")

  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"fetcher processing preRestart,id:$syncTaskId")
    context.become(receive)
    fetcherChangeStatus(Status.RESTARTING)
    super.preRestart(reason, message)
  }

  override def supervisorStrategy = {
    AllForOneStrategy() {
      case e: Exception => {
        fetcherChangeStatus(Status.ERROR)
        log.error(s"fetcherManager crashed,exception:$e,cause:${e.getCause},processing SupervisorStrategy,id:$syncTaskId")
        Escalate
      }
      case error: Error => {
        fetcherChangeStatus(Status.ERROR)
        log.error(s"fetcherManager crashed,error:$error,cause:${error.getCause},processing SupervisorStrategy,id:$syncTaskId")
        Escalate
      }
      case e => {
        log.error(s"fetcherManager crashed,throwable:$e,cause:${e.getCause},processing SupervisorStrategy,id:$syncTaskId")
        fetcherChangeStatus(Status.ERROR)
        Escalate
      }
    }
  }

}
