package com.neighborhood.aka.laplce.estuary.mysql.lifecycle

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, OneForOneStrategy, Props}
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle._
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager
import com.typesafe.config.Config
import org.I0Itec.zkclient.exception.ZkTimeoutException

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by john_liu on 2018/2/1.
  */

class MySqlBinlogController(commonConfig: Config, taskInfoBean: Mysql2KafkaTaskInfoBean) extends SyncController with Actor with ActorLogging {
  //资源管理器，一次同步任务所有的resource都由resourceManager负责
  val resourceManager = Mysql2KafkaTaskInfoManager.buildManager(commonConfig, taskInfoBean)
  val mysql2KafkaTaskInfoManager = resourceManager
  //配置
  val config = context.system.settings.config
  //canal的mysqlConnection
  val mysqlConnection: MysqlConnection = resourceManager.mysqlConnection

  var fetchCount: Long = 0L
  var fetchAvergeTime: Double = 0

  var batchCount: Long = 0L
  var batchAvergeTime: Double = 0

  var sinkCount: Long = 0L
  var sinkAvergeTime: Double = 0


  override var errorCountThreshold: Int = 3
  override var errorCount: Int = 0

  /**
    * 1.初始化HeartBeatsListener
    * 2.初始化binlogSinker
    * 3.初始化binlogEventBatcher
    * 4.初始化binlogFetcher
    */

  //offline 状态
  override def receive: Receive = {
    case "start" => {

      context.become(online)
      startAllWorkers
      log.info("controller switched to online,start all workers")
    }
    case "restart" => {
      context.become(online)
      restartAllWorkers
      log.info("controller switched to online,restart all workers")
    }
    case ListenerMessage(msg) => {
      msg match {
        case "restart" => {
          sender ! SyncControllerMessage("start")
        }
        //        case "reconnect" => {
        //          mysqlConnection.synchronized(mysqlConnection.reconnect())
        //        }
      }
    }

    case SyncControllerMessage(msg) => {
      msg match {
        case "restart" => self ! "restart"
        case _ => {

        }
      }
    }
  }

  def online: Receive = {

    case ListenerMessage(msg) => {
      msg match {
        //        case "restart" => {
        //          sender ! SyncControllerMessage("start")
        //        }
        //        case "reconnect" => {
        //          try {
        //            mysqlConnection.synchronized(mysqlConnection.reconnect())
        //          } catch {
        //            case e: Exception => processError(e, ListenerMessage("reconnect"))
        //          }
        //
        //        }
        case x => {
          log.warning(s"syncController online unhandled message:$x")
        }
      }
    }
    case SinkerMessage(msg) => {
      //      msg match {
      //        case "restart" => {
      //          sender ! SyncControllerMessage("start")
      //        }
      //      }
    }
    case SyncControllerMessage(msg) => {

    }
  }

  /**
    * 重启时调用
    * 与StartAllWorkers的区别是
    * StartAllWorkers的区别是 重启不需要scheduler的定时方法
    */
  def restartAllWorkers = {
    //启动sinker
    context
      .child("binlogSinker")
      .map {
        ref =>
          ref ! SyncControllerMessage("start")
          context.system.scheduler.schedule(5 minutes, 5 minutes, ref, SyncControllerMessage("record"))
      }
    //启动batcher
    context
      .child("binlogBatcher")
      .map {
        ref => context.system.scheduler.scheduleOnce(1 second, ref, SyncControllerMessage("start"))
      }
    //启动fetcher
    context
      .child("binlogFetcher")
      .map {
        ref => context.system.scheduler.scheduleOnce(2 second, ref, SyncControllerMessage("start"))
      }
    //启动listener
    context
      .child("heartBeatsListener")
      .map {
        ref =>
          ref ! SyncControllerMessage("start")
          val queryTimeOut = config.getInt("common.query.timeout")
      }
  }

  def startAllWorkers = {
    //启动recorder
    //    context
    //      .child("binlogPositionRecorder")
    //      .map {
    //        ref =>
    //        // ref ! SyncControllerMessage("start")
    //      }
    //启动sinker
    context
      .child("binlogSinker")
      .map {
        ref => ref ! SyncControllerMessage("start")
      }
    //启动batcher
    context
      .child("binlogBatcher")
      .map {
        ref => context.system.scheduler.scheduleOnce(1 second, ref, SyncControllerMessage("start"))
      }
    //启动fetcher
    context
      .child("binlogFetcher")
      .map {
        ref => context.system.scheduler.scheduleOnce(2 second, ref, SyncControllerMessage("start"))
      }
    //启动listener
    context
      .child("heartBeatsListener")
      .map {
        ref =>
          ref ! SyncControllerMessage("start")
          val queryTimeOut = config.getInt("common.query.timeout")
          //开始之后每`queryTimeOut`毫秒一次
          context.system.scheduler.schedule(queryTimeOut milliseconds, queryTimeOut milliseconds, ref, ListenerMessage("listen"))
      }
  }

  def initWorkers = {

    //初始化HeartBeatsListener
    log.info("initialize listener")
    context.actorOf(MysqlConnectionListener.props(mysql2KafkaTaskInfoManager).withDispatcher("akka.pinned-dispatcher"), "heartBeatsListener")
    //    //初始化binlogPositionRecorder
    //    val recorder = context.actorOf(MysqlBinlogPositionRecorder.props(mysql2KafkaTaskInfoManager), "binlogPositionRecorder")
    //初始化binlogSinker
    //如果并行打开使用并行sinker

    val binlogSinker = if (resourceManager.taskInfo.isTransactional) {
      log.info("initialize sinker with mode transactional ")
      //使用transaction式
      context.actorOf(Props(classOf[BinlogTransactionBufferSinker], resourceManager), "binlogSinker")
    } else {
      log.info("initialize sinker with mode concurrent ")
      context.actorOf(ConcurrentBinlogSinker.prop(resourceManager), "binlogSinker")
    }
    log.info("initialize batcher")
    //初始化binlogEventBatcher
    val binlogEventBatcher = context.actorOf(BinlogEventBatcher.prop(binlogSinker, resourceManager), "binlogBatcher")
    log.info("initialize fetcher")
    //初始化binlogFetcher
    context.actorOf(Props(classOf[MysqlBinlogFetcher], resourceManager, binlogEventBatcher).withDispatcher("akka.pinned-dispatcher"), "binlogFetcher")
  }

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {

    //todo 记录log
    errorCount += 1
    if (isCrashed) {
      switch2Error
      errorCount = 0
      throw new Exception("syncController error for 3 times")
    } else {
      message
      self ! message
    }
  }

  /**
    * ********************* 状态变化 *******************
    */
  private def switch2Offline = {
    mysql2KafkaTaskInfoManager.syncControllerStatus = Status.OFFLINE
  }

  private def switch2Busy = {
    mysql2KafkaTaskInfoManager.syncControllerStatus = Status.BUSY
  }

  private def switch2Error = {
    mysql2KafkaTaskInfoManager.syncControllerStatus = Status.ERROR
  }

  private def switch2Free = {
    mysql2KafkaTaskInfoManager.syncControllerStatus = Status.FREE
  }

  private def switch2Restarting = {
    mysql2KafkaTaskInfoManager.syncControllerStatus = Status.RESTARTING
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
    log.info("start init all workers")
    initWorkers
  }

  //正常关闭时会调用，关闭资源
  override def postStop(): Unit

  = {
    //todo logstash
    //    mysqlConnection.disconnect()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit

  = {
    log.info("syncController processing preRestart")
    //默认的话是会调用postStop，preRestart可以保存当前状态
    context.become(receive)
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit

  = {
    log.info("syncController processing postRestart")
    //可以恢复之前的状态，默认会调用
    super.postRestart(reason)
    log.info("syncController will restart in 1 minute")
    context.system.scheduler.scheduleOnce(1 minute, self, SyncControllerMessage("restart"))
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: ZkTimeoutException => {
        Restart
        //todo log
      }
      case e: Exception => Restart
      case error: Error => Restart
      case _ => Restart
    }
  }
}

object MySqlBinlogController {
  def props(commonConfig: Config, taskInfoBean: Mysql2KafkaTaskInfoBean): Props = {
    Props(new MySqlBinlogController(commonConfig, taskInfoBean))
  }
}
