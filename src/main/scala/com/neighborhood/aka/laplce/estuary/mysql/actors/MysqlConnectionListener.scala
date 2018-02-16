package com.neighborhood.aka.laplce.estuary.mysql.actors

import akka.actor.{Actor, Props, SupervisorStrategy}
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.core.lifecycle.{HeartBeatListener, ListenerMessage, SyncControllerMessage}
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.sys.Prop
import scala.util.Try

/**
  * Created by john_liu on 2018/2/1.
  */
class MysqlConnectionListener(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) extends Actor with HeartBeatListener {

  //数据库连接
  val connection: Option[MysqlConnection] = Option(mysql2KafkaTaskInfoManager.mysqlConnection)
  //配置
  val config = context.system.settings.config
  //监听心跳用sql
  val delectingSql = config.getString("common.delect.sql")
  //慢查询阈值
  val queryTimeOut = config.getInt("common.query.timeout")
  //重试次数
  var retryTimes = config.getInt("common.process.retrytime")
  // 状态位
  //var state      =

  //等待初始化 offline状态
  override def receive: Receive = {

    case SyncControllerMessage(msg) => {
      msg match {
        case "start" => {
          //todo logstash

          //变为online状态
          context.become(onlineState)

        }
        case "stop" => {
        }
        case "state" => context.parent ! ListenerMessage("state:offline")
      }
    }
    case ListenerMessage(msg) => {
      //todo logstash
      msg match {
        case x => {
          println(x)
        }
      }


    }
  }

  //onlineState
  def onlineState: Receive = {
    case ListenerMessage(msg) => {
      msg match {
        case "listen" => {
          //          //测试
          //          if(System.currentTimeMillis()%2 ==0){
          //            throw new Exception("偶数异常")
          //          }
          println("is listening to the heartbeats")
          listenHeartBeats
        }
        case "stop" => {
          //变为offline状态
          context.become(receive)
        }
      }
    }
    case SyncControllerMessage(msg: String) => {
      msg match {
        case "stop" => {
          //变为offline状态
          context.become(receive)
        }
        case "state" => context.parent ! ListenerMessage("state:online")

      }
    }
  }

  def listenHeartBeats: Unit = {
    //todo connection None情况
    connection.map {
      conn =>
        val before = System.currentTimeMillis
        if (!Try(conn
          .query(delectingSql)).isSuccess) {
          retryTimes = retryTimes - 1
          if (retryTimes <= 0) {
            self ! ListenerMessage("stop")
            context.parent ! ListenerMessage("reconnect")
            context.parent ! ListenerMessage("restart")
            retryTimes = config.getInt("common.process.retrytime")
          }
        } else {
          val after = System.currentTimeMillis()
          val duration = after - before
          //todo 记录时间
        }
    }

  }
  /**
    * ********************* 状态变化 *******************
    */
  /**
    * **************** Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    //开始之后每`queryTimeOut`毫秒一次
    context.system.scheduler.schedule(queryTimeOut milliseconds, queryTimeOut milliseconds, self, ListenerMessage("listen"))
  }

  override def postStop(): Unit = super.postStop()

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    //todo logstash
    super.preRestart(reason, message)
    context.parent ! ListenerMessage("restart")
  }

  override def postRestart(reason: Throwable): Unit = super.postRestart(reason)
}

object MysqlConnectionListener {
  def props(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager): Props = {
    Props(new MysqlConnectionListener(mysql2KafkaTaskInfoManager))
  }
}

