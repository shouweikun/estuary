package com.neighborhood.aka.laplce.estuary.mysql.lifecycle

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props}
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle.{HeartBeatListener, ListenerMessage, Status, SyncControllerMessage}
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager
import com.typesafe.config.Config

import scala.util.Try

/**
  * Created by john_liu on 2018/2/1.
  */
class MysqlConnectionListener(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) extends Actor with HeartBeatListener with ActorLogging {


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
          switch2Online
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
          switch2Offline
        }
      }
    }
    case SyncControllerMessage(msg: String) => {
      msg match {
        case "stop" => {
          //变为offline状态
          context.become(receive)
          switch2Offline
        }
      }
    }
  }

  def listenHeartBeats: Unit = {
    //todo connection None情况
    connection.foreach {
      conn =>
        val before = System.currentTimeMillis
        if (!Try(conn
          .query(delectingSql)).isSuccess) {

          retryTimes = retryTimes - 1
          if (retryTimes <= 0) {
            self ! ListenerMessage("stop")
            context.parent ! ListenerMessage("reconnect")
            context.parent ! ListenerMessage("restart")
            retryTimes = this.retryTimeThreshold
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
  private def switch2Offline = {
    mysql2KafkaTaskInfoManager.heartBeatListenerStatus = Status.OFFLINE
  }

  private def switch2Online = {
    mysql2KafkaTaskInfoManager.heartBeatListenerStatus = Status.ONLINE
  }

  private def switch2Error = {
    mysql2KafkaTaskInfoManager.heartBeatListenerStatus = Status.ERROR
  }

  private def switch2Restarting = {
    mysql2KafkaTaskInfoManager.heartBeatListenerStatus = Status.RESTARTING
  }


  /**
    * **************** Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    switch2Offline
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    //todo logstash
    context.become(receive)
    switch2Restarting
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    switch2Restarting
    context.parent ! ListenerMessage("restart")
    super.postRestart(reason)
  }

  override def postStop(): Unit = {
    connection.get.disconnect()
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: Exception => {
        switch2Error
        Escalate
      }
      case _: Error => {
        switch2Error
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

