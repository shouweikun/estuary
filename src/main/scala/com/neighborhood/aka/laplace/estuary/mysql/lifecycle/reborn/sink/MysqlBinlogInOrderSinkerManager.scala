package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{ActorRef, OneForOneStrategy, Props}
import com.neighborhood.aka.laplace.estuary.bean.exception.other.WorkerInitialFailureException
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SinkerMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinkerCommand.{MysqlInOrderSinkerGetAbnormal, MysqlInOrderSinkerStart}
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp
import org.I0Itec.zkclient.exception.ZkTimeoutException

/**
  * Created by john_liu on 2018/5/8.
  */

abstract class MysqlBinlogInOrderSinkerManager(
                                                val taskManager: MysqlSinkManagerImp with TaskManager
                                              ) extends SourceDataSinkerManagerPrototype[MysqlSinkFunc] {

  /**
    * 是否是顶部
    */
  override val isHead: Boolean = true

  /**
    * 资源管理器
    *
    * @return
    */
  override val sinkManager: SinkManager[MysqlSinkFunc] = taskManager

  /**
    * sinker的数量
    */
  override val sinkerNum: Int = taskManager.sinkerNum

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId
  /**
    * 位置管理器
    */
  lazy val positonRecorder: Option[ActorRef] = taskManager.positionRecorder

  override def receive: Receive = {
    case SyncControllerMessage(MysqlInOrderSinkerStart) => start
  }

  def online: Receive = {
    case x@SinkerMessage(_: MysqlInOrderSinkerGetAbnormal) => send2Recorder(x)
  }


  protected def handleSinkerError(x: SinkerMessage) = {
    send2Recorder(x)
    switch2Error
  }

  /**
    * 错位次数阈值
    */
  override val errorCountThreshold: Int = 1
  /**
    * 错位次数
    */
  override var errorCount: Int = 0

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???


  /**
    * ********************* 状态变化 *******************
    */

  override def changeFunc(status: Status) = TaskManager.changeFunc(status, taskManager)

  override def onChangeFunc = TaskManager.onChangeStatus(taskManager)

  override def sinkerChangeStatus(status: Status) = TaskManager.changeStatus(status, changeFunc, onChangeFunc)


  /**
    * **************** Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    sinkerChangeStatus(Status.OFFLINE)
    initSinkers
    log.info(s"switch sinker to offline,id:$syncTaskId")

  }

  override def postStop(): Unit = {
    log.info(s"sinker processing postStop,id:$syncTaskId")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"sinker processing preRestart,id:$syncTaskId")
    sinkerChangeStatus(Status.ERROR)
    context.become(receive)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"sinker processing preRestart,id:$syncTaskId")
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

object MysqlBinlogInOrderSinkerManager {
  def buildMysqlBinlogInOrderMysqlSinkerManager(taskManager: MysqlSinkManagerImp with TaskManager, name: String): Props = name match {
    case MysqlBinlogInOrderMysqlSinkerManager.name => MysqlBinlogInOrderMysqlSinkerManager.props(taskManager)
    case MysqlBinlogInOrderMysqlSinkerSinkByNameManager.name => MysqlBinlogInOrderMysqlSinkerSinkByNameManager.props(taskManager)
    case _ => throw new WorkerInitialFailureException(s"cannot build MysqlBinlogInOrderMysqlSinkerManager name item match $name")
  }
}