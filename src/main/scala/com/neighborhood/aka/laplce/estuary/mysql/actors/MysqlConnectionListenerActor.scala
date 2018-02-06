package com.neighborhood.aka.laplce.estuary.mysql.actors

import akka.actor.{Actor, SupervisorStrategy}
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection

import scala.concurrent.duration._
import scala.util.Try

/**
  * Created by john_liu on 2018/2/1.
  */
class MysqlConnectionListenerActor(conn:MysqlConnection) extends Actor {

  //数据库连接
  var connection: Option[MysqlConnection] = Option(conn)
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
    case conn: MysqlConnection => {
      connection = Option(conn)
    }
    case EventParserMessage(msg) => {
      msg match {
        case "start" => {
          //todo logstash
          //必须保证Connection不为空
          if(connection == None){
            context.parent ! "connection"
          }else{
            //变为online状态
            context.become(onlineState)
            //开始之后每`queryTimeOut`毫秒一次
            context.system.scheduler.schedule(0 milliseconds, queryTimeOut milliseconds, self, ListenerMessage("listen"))
          }
        }
        case "stop" => {
          //connection清空
          connection = None
        }
        case "state" => context.parent ! ListenerMessage("state:offline")
      }
    }
    case ListenerMessage(msg) => {
      //todo logstash
      case "stop" => {
        //connection清空
        connection = None
      }


    }
  }

  //onlineState
  def onlineState: Receive = {
    case ListenerMessage(msg) => {
      msg match {
        case "listen" => {
          listenHeartBeats
        }
        case "stop" => {
          //connection清空
          connection = None
          //变为offline状态
          context.become(receive)
        }
      }
    }
    case EventParserMessage(msg: String) => {
      msg match {
        case "stop" => {
          //connection清空
          connection = None
          //变为offline状态
          context.become(receive)
        }
        case "state" => context.parent ! ListenerMessage("state:online")

      }
    }
  }

  def listenHeartBeats :Unit= {
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
            retryTimes = config.getInt("common.query.retrytime")
          }
        } else {
          val after = System.currentTimeMillis()
          val duration = after - before
          //todo 记录时间
        }
    }

  }

  override def preStart(): Unit = {

  }

  override def postStop(): Unit = super.postStop()

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    //todo logstash
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = super.postRestart(reason)
}

