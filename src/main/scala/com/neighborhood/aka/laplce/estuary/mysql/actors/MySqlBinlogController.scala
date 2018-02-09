package com.neighborhood.aka.laplce.estuary.mysql.actors

import akka.actor.{Actor, Props}
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle.{ListenerMessage, SyncController, SyncControllerMessage}
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/2/1.
  */

class MySqlBinlogController[EVENT](rm: Mysql2KafkaTaskInfoManager) extends SyncController with Actor {
  //资源管理器，一次同步任务所有的resource都由resourceManager负责
  val resourceManager = rm
  //配置
  val config = context.system.settings.config
  //canal的mysqlConnection
  val mysqlConnection: MysqlConnection = resourceManager.mysqlConnection
  override var errorCountThreshold: Int = 3
  override var errorCount: Int = 0

  //offline 状态
  override def receive: Receive = {
    case "start" => {
      context.become(online)
      startAllWorkers
    }
    case ListenerMessage(msg) => {
      msg match {
        case "connection" => {
          //todo logStash
          sender() ! mysqlConnection
        }
        case "reconnect" => {
          mysqlConnection.synchronized(mysqlConnection.reconnect())
        }
      }
    }
  }

  def online: Receive = {

    case ListenerMessage(msg) => {
      msg match {
        case "connection" => {
          //todo logStash
          sender() ! mysqlConnection
          sender() ! SyncControllerMessage("start")
        }
        case "reconnect" => {
          mysqlConnection.reconnect()
        }
      }
    }

    case SyncControllerMessage(msg) => {

    }
  }
   def startAllWorkers = {
       context
       .child("binlogSinker")
       .map{
         ref => ref ! SyncControllerMessage("start")
       }
       context
       .child("binlogBatcher")
       .map{
         ref => ref ! SyncControllerMessage("start")
       }
       context
         .child("binlogFetcher")
         .map{
           ref => ref ! SyncControllerMessage("start")
         }

       context
       .child("heartBeatsListener")
       .map{
         ref => ref ! SyncControllerMessage("start")
       }
   }
  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {
    //do nothing
  }

  /**
    * **************** Actor生命周期 *******************
    */

  /**
    * 每次启动都会调用，在构造器之后调用
    * 1.初始化HeartBeatsListener
    * 2.初始化binlogSinker
    * 3.初始化binlogEventBatcher
    * 4.初始化binlogFetcher
    */
  override def preStart(): Unit

  = {
    //todo logstash
    //初始化HeartBeatsListener
    context.actorOf(Props(classOf[MysqlConnectionListener], resourceManager), "heartBeatsListener")
    //初始化binlogSinker
    //如果并行打开使用并行sinker
    val binlogSinker = if (resourceManager.taskInfo.isTransactional) {

    } else {
      //不是然使用transaction式
      context.actorOf(Props(classOf[BinlogTransactionBufferSinker], resourceManager), "binlogSinker")
    }
    //初始化binlogEventBatcher
    val binlogEventBatcher = context.actorOf(Props(classOf[BinlogEventBatcher], resourceManager, binlogSinker)
      , "binlogBatcher")
    //初始化binlogFetcher
    context.actorOf(Props(classOf[MysqlBinlogFetcher], resourceManager, binlogEventBatcher), "binlogFetcher")
  }

  //正常关闭时会调用，关闭资源
  override def postStop(): Unit

  = {
    //todo logstash
    mysqlConnection.disconnect()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit

  = {
    //todo logstash
    //默认的话是会调用postStop，preRestart可以保存当前状态
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit

  = {
    //todo logstash
    //可以恢复之前的状态，默认会调用
    super.postRestart(reason)
  }

}
