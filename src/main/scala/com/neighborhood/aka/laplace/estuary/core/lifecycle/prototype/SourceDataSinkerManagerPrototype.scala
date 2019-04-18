package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import akka.actor.{ActorRef, OneForOneStrategy}
import akka.actor.SupervisorStrategy.Escalate
import com.neighborhood.aka.laplace.estuary.bean.exception.control.WorkerCannotFindException
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.{SourceDataSinker, Status}
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import org.I0Itec.zkclient.exception.ZkTimeoutException

/**
  * Created by john_liu on 2018/5/21.
  */
trait SourceDataSinkerManagerPrototype[B <: SinkFunc] extends ActorPrototype with SourceDataSinker {
  /**
    * sinker名称 用于动态加载
    */
  def sinkerName: String

  /**
    * 加载的sinker的名称
    */
  def sinkerNameToLoad: Map[String, String] = taskManager.sinkerNameToLoad

  /**
    * position记录器
    */
  def positionRecorder: Option[ActorRef]

  /**
    * 是否是顶部
    */
  def isHead: Boolean

  /**
    * 任务信息管理器
    */
  def taskManager: TaskManager

  /**
    * 资源管理器
    *
    */
  def sinkManager: SinkManager[B]

  /**
    * sinker的数量
    */
  def sinkerNum: Int

  /**
    * sinker的list
    */
  def sinkList = sinkList_

  lazy val sinkList_ = context.children.toList

  /**
    * 在线模式
    *
    * @return
    */
  protected def online: Receive

  /**
    * 错误模式，可以不实现
    *
    * @return
    */
  protected def error: Receive = {
    case x => log.warning(s"sinker is crashed,cannot handle this message:$x,id:$syncTaskId")
  }

  /**
    * 初始化sinkers
    */
  protected def initSinkers: Unit

  /**
    * 切换为开始模式
    */
  protected def start: Unit = {
    //online模式
    log.info(s"sinker swtich to online,id:$syncTaskId")
    context.become(online)
    sinkerChangeStatus(Status.ONLINE)
  }

  /**
    * 切换为Error
    */
  protected def switch2Error: Unit = {
    log.info(s"sinker swtich to error,id:$syncTaskId")
    context.become(error)
    sinkerChangeStatus(Status.ERROR)
  }

  /**
    * 想recorder发送信息
    *
    * @param message
    */
  protected def send2Recorder(message: Any): Unit = positionRecorder.fold(throw new WorkerCannotFindException(s"cannot find positionRecorder when send message:$message to it,id:$syncTaskId"))(ref => ref ! message)

  protected def changeFunc(status: Status) = TaskManager.changeFunc(status, taskManager)

  protected def onChangeFunc = TaskManager.onChangeStatus(taskManager)

  protected def sinkerChangeStatus(status: Status) = TaskManager.changeStatus(status, changeFunc, onChangeFunc)

  /**
    * **************** Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    sinkerChangeStatus(Status.OFFLINE)
    initSinkers

  }

  override def postStop(): Unit = {
    log.info(s"sinker processing postStop,id:$syncTaskId")

    //    kafkaSinker.kafkaProducer.close()
    //    sinkTaskPool.environment.shutdown()
    //logPositionHandler.logPositionManage
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"sinker processing preRestart,id:$syncTaskId")
    sinkerChangeStatus(Status.ERROR)
    context.become(receive)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"sinker processing postRestart,id:$syncTaskId")
    sinkerChangeStatus(Status.RESTARTING)
    super.postRestart(reason)
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: ZkTimeoutException => {
        sinkerChangeStatus(Status.ERROR)
        log.error(s"can not connect to zookeeper server,id:$syncTaskId")
        Escalate
      }
      case e: Exception => {
        sinkerChangeStatus(Status.ERROR)
        log.error(s"sinker crashed,exception:$e,cause:${e.getCause},processing SupervisorStrategy,id:$syncTaskId")
        Escalate
      }
      case error: Error => {
        sinkerChangeStatus(Status.ERROR)
        log.error(s"sinker crashed,error:$error,cause:${error.getCause},processing SupervisorStrategy,id:$syncTaskId")
        Escalate
      }
      case e => {
        log.error(s"sinker crashed,throwable:$e,cause:${e.getCause},processing SupervisorStrategy,id:$syncTaskId")
        sinkerChangeStatus(Status.ERROR)
        Escalate
      }
    }
  }
}
