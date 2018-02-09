package com.neighborhood.aka.laplce.estuary.mysql.actors

import java.io.IOException

import akka.actor.{Actor, Props}
import com.alibaba.otter.canal.parse.CanalEventParser
import com.alibaba.otter.canal.parse.inbound.mysql.{AbstractMysqlEventParser, MysqlConnection}
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplce.estuary.core.lifecycle.{ListenerMessage, SyncController, SyncControllerMessage}
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager
import org.apache.commons.lang.StringUtils

/**
  * Created by john_liu on 2018/2/1.
  */

class MySqlBinlogController[EVENT](rm: Mysql2KafkaTaskInfoManager) extends AbstractMysqlEventParser with CanalEventParser[EVENT] with SyncController with Actor {
  //资源管理器，一次同步任务所有的resource都由resourceManager负责
  val resourceManager = rm
  //配置
  val config = context.system.settings.config
  //canal的mysqlConnection
  val mysqlConnection: MysqlConnection = buildErosaConnection()
  val dumpErrorCountThreshold = 3
  var dumpErrorCount = 0
  val retryBackoff = Option(config.getInt("common.retry.backoff")).getOrElse(30)
  var entryPosition: EntryPosition = null


  //offline 状态
  override def receive: Receive = {
    case "start" => {
      context.become(online)

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

  def retryBack: Receive = {
    case "restart" => {
      context.become(receive)
      self ! "start"
    }
  }

  /**
    * @return 返回MysqlConnection 而不是 其父类ErosaConnection
    *
    */
  override def buildErosaConnection(): MysqlConnection = {
    resourceManager.mysqlConnection
  }

  /**
    *
    *
    */
  def retryCountdown: Boolean = {
    dumpErrorCountThreshold >= 0 && dumpErrorCount > dumpErrorCountThreshold
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
  override def preStart(): Unit = {
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
      , "binlogEventBatcher")
    //初始化binlogFetcher
    context.actorOf(Props(classOf[MysqlBinlogFetcher], resourceManager, binlogEventBatcher), "binlogFetcher")
  }

  //正常关闭时会调用，关闭资源
  override def postStop(): Unit = {
    //todo logstash
    mysqlConnection.disconnect()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    //todo logstash
    //默认的话是会调用postStop，preRestart可以保存当前状态
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    //todo logstash
    //可以恢复之前的状态，默认会调用
    super.postRestart(reason)
  }

  override protected def processDumpError(e: Throwable): Unit = {
    if (e.isInstanceOf[IOException]) {
      val message = e.getMessage
      if (StringUtils.contains(message, "errno = 1236")) { // 1236 errorCode代表ER_MASTER_FATAL_ERROR_READING_BINLOG
        dumpErrorCount += 1
      }
    }
  }
}
