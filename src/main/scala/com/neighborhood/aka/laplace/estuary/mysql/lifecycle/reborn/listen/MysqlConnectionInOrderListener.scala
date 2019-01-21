package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.listen

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.HeartBeatListenerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{ListenerMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.{SourceManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.listen.MysqlBinlogInOrderListenerCommand.{MysqlBinlogInOrderListenerListen, MysqlBinlogInOrderListenerStart, MysqlBinlogInOrderListenerStop}
import com.neighborhood.aka.laplace.estuary.mysql.source.{MysqlConnection, MysqlSourceManagerImp}

import scala.util.Try

/**
  * Created by john_liu on 2018/2/1.
  *
  * 心跳监听
  * 对数据源进行心跳监听
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlConnectionInOrderListener(
                                            override val taskManager: MysqlSourceManagerImp with TaskManager
                                          ) extends HeartBeatListenerPrototype[MysqlConnection] {


  /**
    * 资源管理器
    */
  override val sourceManager: SourceManager[MysqlConnection] = taskManager
  /**
    * syncTaskId
    */
  override val syncTaskId = taskManager.syncTaskId
  /**
    * 数据库连接
    */
  lazy val mysqlConnection: Option[MysqlConnection] = Option(connection.asInstanceOf[MysqlConnection])
  /**
    * 监听心跳用sql
    */
  val detectingSql: String = taskManager.detectingSql
  /**
    * 重试次数标准值
    */
  val retryTimeThreshold = taskManager.listenRetryTime
  /**
    * 重试次数
    */
  var retryTimes: Int = retryTimeThreshold

  //等待初始化 offline状态
  override def receive: Receive = {
    case SyncControllerMessage(MysqlBinlogInOrderListenerStart) => start
    case SyncControllerMessage(MysqlBinlogInOrderListenerListen) => start
  }

  //onlineState
  def onlineState: Receive = {
    case SyncControllerMessage(MysqlBinlogInOrderListenerListen) => listenHeartBeats
    case SyncControllerMessage(MysqlBinlogInOrderListenerStop) => stop
    case ListenerMessage(MysqlBinlogInOrderListenerListen) => listenHeartBeats
    case ListenerMessage(MysqlBinlogInOrderListenerStop) => stop

  }


  protected def listenHeartBeats: Unit = {
    mysqlConnection.foreach {
      conn =>
        val before = System.currentTimeMillis
        if (!Try(conn
          .query(detectingSql)).isSuccess) {

          retryTimes = retryTimes - 1
          if (retryTimes <= 0) {
            throw new RuntimeException(s"heartBeatListener connot listen!,id:$syncTaskId")
          }
        } else {

          lazy val after = System.currentTimeMillis()
          lazy val duration = after - before

          if (duration > 50) log.warning(s"slow listen,$duration,id:$syncTaskId") else log.debug(s"this listening test cost :$duration,id:$syncTaskId")
          //这样的话，发现正常了就重置计数器
          retryTimes = retryTimeThreshold
        }
    }


  }


  /**
    * ********************* 状态变化 *******************
    */
  override def changeFunc(status: Status) = TaskManager.changeFunc(status, taskManager)

  override def onChangeFunc = TaskManager.onChangeStatus(taskManager)

  override def listenerChangeStatus(status: Status) = TaskManager.changeStatus(status, changeFunc, onChangeFunc)


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
    mysqlConnection.get.disconnect()
  }


  /** ********************* 未被使用 ************************/
  override def errorCountThreshold: Int = 1
  override var errorCount: Int = 0

  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {
    //do Nothing
  }

  /** ********************* 未被使用 ************************/


}

object MysqlConnectionInOrderListener {
  def props(taskManager: MysqlSourceManagerImp with TaskManager): Props = Props(new MysqlConnectionInOrderListener(taskManager))
}


