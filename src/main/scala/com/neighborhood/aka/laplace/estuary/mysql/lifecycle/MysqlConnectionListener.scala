package com.neighborhood.aka.laplace.estuary.mysql.lifecycle

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{HeartBeatListener, ListenerMessage, Status, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager

import scala.util.Try

/**
  * Created by john_liu on 2018/2/1.
  */
class MysqlConnectionListener(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) extends Actor with HeartBeatListener with ActorLogging {
  /**
    * syncTaskId
    */
  val syncTaskId = mysql2KafkaTaskInfoManager.syncTaskId
  /**
    * 数据库连接
    */
  val connection: Option[MysqlConnection] = Option(mysql2KafkaTaskInfoManager.mysqlConnection)
  /**
    * 监听心跳用sql
    */
  val delectingSql: String = mysql2KafkaTaskInfoManager.taskInfo.detectingSql
  /**
    * 重试次数标准值
    */
  var retryTimeThreshold = mysql2KafkaTaskInfoManager.taskInfo.listenRetrytime
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
          log.info("heartBeatListener swtich to online")
          context.become(onlineState)
          connection.get.connect()
          listenerChangeStatus(Status.ONLINE)
        }
        case "stop" => {
          //doNothing
        }
        case str => {
          if (str == "listen") self ! SyncControllerMessage("start")
          log.warning(s"listener offline  unhandled message:$str")
        }
      }
    }
    case ListenerMessage(msg) => {
      msg match {
        case str => {

          log.warning(s"listener offline  unhandled message:$str")
        }
      }


    }
  }

  //onlineState
  def onlineState: Receive = {
    case ListenerMessage(msg) => {
      msg match {
        case "listen" => {
          log.info(s"is listening to the heartbeats")
          listenHeartBeats

        }
        case "stop" => {
          //变为offline状态
          context.become(receive)
          listenerChangeStatus(Status.OFFLINE)
        }
      }
    }
    case SyncControllerMessage(msg: String) => {
      msg match {
        case "stop" => {
          //变为offline状态
          context.become(receive)
          listenerChangeStatus(Status.OFFLINE)
        }
        case _ => log.warning(s"listener online unhandled message:$msg,id:$syncTaskId")
      }
    }
  }

  def listenHeartBeats: Unit = {
    connection.foreach {
      conn =>
        val before = System.currentTimeMillis
        if (!Try(conn
          .query(delectingSql)).isSuccess) {

          retryTimes = retryTimes - 1
          if (retryTimes <= 0) {
            throw new RuntimeException(s"listener connot listen!,id:$syncTaskId")
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
  private def changeFunc(status: Status) = TaskManager.changeFunc(status, mysql2KafkaTaskInfoManager)

  private def onChangeFunc = TaskManager.onChangeStatus(mysql2KafkaTaskInfoManager)

  private def listenerChangeStatus(status: Status) = TaskManager.changeStatus(status, changeFunc, onChangeFunc)


  /**
    * **************** Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    listenerChangeStatus(Status.OFFLINE)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.become(receive)
    listenerChangeStatus(Status.RESTARTING)
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
  }

  override def postStop(): Unit = {
    connection.get.disconnect()
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: Exception => {
        listenerChangeStatus(Status.ERROR)
        Escalate
      }
      case _ => {
        listenerChangeStatus(Status.ERROR)
        Escalate
      }
    }
  }


  /** ********************* 未被使用 ************************/
  override var errorCountThreshold: Int = _
  override var errorCount: Int = _

  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {
    //do Nothing
  }

  /** ********************* 未被使用 ************************/


}

object MysqlConnectionListener {
  def props(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager): Props = {
    Props(new MysqlConnectionListener(mysql2KafkaTaskInfoManager))
  }
}

