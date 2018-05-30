package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.HeartBeatListenerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{ListenerMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.{RecourceManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager

import scala.util.Try

/**
  * Created by john_liu on 2018/2/1.
  */
class MysqlConnectionInOrderListener(
                                      override val taskManager: Mysql2KafkaTaskInfoManager
                                    ) extends HeartBeatListenerPrototype[MysqlConnection] {


  /**
    * 资源管理器
    */
  override val recourceManager: RecourceManager[_, MysqlConnection, _] = taskManager
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
  val delectingSql: String = taskManager.taskInfo.detectingSql
  /**
    * 重试次数标准值
    */
  var retryTimeThreshold = taskManager.taskInfo.listenRetrytime
  /**
    * 重试次数
    */
  var retryTimes: Int = retryTimeThreshold

  //等待初始化 offline状态
  override def receive: Receive = {

    case SyncControllerMessage(msg) => {
      msg match {
        case "start" => {

          //变为online状态
          log.info(s"heartBeatListener swtich to online,id:$syncTaskId")
          context.become(onlineState)
          mysqlConnection.fold {
            log.error(s"heartBeatListener connection cannot be null,id:$syncTaskId");
            throw new Exception(s"listener connection cannot be null,id:$syncTaskId")
          }(conn => conn.connect())
          listenerChangeStatus(Status.ONLINE)
        }
        case str => {
          if (str == "listen") self ! SyncControllerMessage("start")
          log.warning(s"heartBeatListener offline  unhandled message:$str,id:$syncTaskId")
        }
      }
    }
    case ListenerMessage(msg) => {
      msg match {
        case str => {
          log.warning(s"heartBeatListener offline  unhandled message:$str,id:$syncTaskId")
        }
      }


    }
  }

  //onlineState
  def onlineState: Receive = {
    case ListenerMessage(msg) => {
      msg match {
        case "listen" => {
          log.debug(s"heartBeatListener is listening to the heartbeats,$syncTaskId")
          listenHeartBeats
        }
        case "stop" => {
          //变为offline状态
          //未使用
          context.become(receive)
          listenerChangeStatus(Status.OFFLINE)
        }
      }
    }
    case SyncControllerMessage(msg) => {
      msg match {
        case "stop" => {
          //变为offline状态
          //未使用
          context.become(receive)
          listenerChangeStatus(Status.OFFLINE)
        }
        case _ => log.warning(s"listener online unhandled message:$msg,id:$syncTaskId")
      }
    }
  }

  def listenHeartBeats: Unit = {
    mysqlConnection.foreach {
      conn =>
        val before = System.currentTimeMillis
        if (!Try(conn
          .query(delectingSql)).isSuccess) {

          retryTimes = retryTimes - 1
          if (retryTimes <= 0) {
            throw new RuntimeException(s"heartBeatListener connot listen!,id:$syncTaskId")
          }
        } else {

          val after = System.currentTimeMillis()
          val duration = after - before
          log.info(s"this listening test cost :$duration")
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
  override var errorCountThreshold: Int = _
  override var errorCount: Int = _

  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {
    //do Nothing
  }

  /** ********************* 未被使用 ************************/


}

object MysqlConnectionInOrderListener {
  def props(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager): Props = Props(new MysqlConnectionInOrderListener(mysql2KafkaTaskInfoManager))
}


