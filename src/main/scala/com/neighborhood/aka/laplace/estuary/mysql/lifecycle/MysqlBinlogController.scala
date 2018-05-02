package com.neighborhood.aka.laplace.estuary.mysql.lifecycle

import java.util.concurrent.Executors

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorLogging, AllForOneStrategy, OneForOneStrategy, Props}
import akka.routing.RoundRobinPool
import com.neighborhood.aka.laplace.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplace.estuary.core.akkaUtil.PowerAdapter
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{Status, _}
import com.neighborhood.aka.laplace.estuary.core.source.MysqlConnection
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.{Mysql2KafkaTaskInfoManager, SettingConstant}
import com.neighborhood.aka.laplace.estuary.mysql.akkaUtil.DivideDDLRoundRobinRoutingGroup
import org.I0Itec.zkclient.exception.ZkTimeoutException

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Created by john_liu on 2018/2/1.
  */

class MysqlBinlogController(taskInfoBean: Mysql2KafkaTaskInfoBean) extends SyncController with Actor with ActorLogging {
  //资源管理器，一次同步任务所有的resource都由resourceManager负责
  val resourceManager = Mysql2KafkaTaskInfoManager.buildManager(taskInfoBean)
  val mysql2KafkaTaskInfoManager = resourceManager
  //canal的mysqlConnection
  val mysqlConnection: MysqlConnection = resourceManager.mysqlConnection
  implicit val scheduleTaskPool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

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
      //      throw new Exception
      context.become(online)
      controllerChangeStatus(Status.ONLINE)
      startAllWorkers

      log.info("controller switched to online,start all workers")
    }
    case "restart" => {
      context.become(online)
      controllerChangeStatus(Status.ONLINE)
      startAllWorkers
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
    case "start" => startAllWorkers
    case "restart" => throw new RuntimeException("重启")
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
      msg match {
        case "error" => {
          throw new RuntimeException("sinker went wrong when sending data")
        }
        case "flush" => {
          context
            .child("binlogBatcher")
        }
        case "flushDdl" => {
          context
            .child("ddlHandler")
            .map(ref => ref ! SyncControllerMessage("flush"))
        }
        case _ => {}
      }
    }
    case SyncControllerMessage(msg) => {
      msg match {
        //实质上没有上
        case "cost" => {
          if (taskInfoBean.isCosting) {
            context
              .child("powerAdapter")
              .map(ref => ref ! SyncControllerMessage("cost"))
            context.system.scheduler.scheduleOnce(SettingConstant.COMPUTE_COST_CONSTANT seconds, self, SyncControllerMessage("cost"))
          }
        }
        case _ => {}
      }
    }

  }

  //  /**
  //    * 重启时调用
  //    * 与StartAllWorkers的区别是
  //    * StartAllWorkers的区别是 重启不需要scheduler的定时方法
  //    */
  //  def restartAllWorkers = {
  //    //启动sinker
  //    context
  //      .child("binlogSinker")
  //      .map {
  //        ref =>
  //          ref ! SyncControllerMessage("start")
  //          context.system.scheduler.schedule(5 minutes, 5 minutes, ref, SyncControllerMessage("record"))
  //      }
  //    //启动batcher
  //    context
  //      .child("binlogBatcher")
  //      .map {
  //        ref => context.system.scheduler.scheduleOnce(1 second, ref, akka.routing.Broadcast(SyncControllerMessage("start")))
  //      }
  //    //启动fetcher
  //    context
  //      .child("binlogFetcher")
  //      .map {
  //        ref => context.system.scheduler.scheduleOnce(2 second, ref, SyncControllerMessage("start"))
  //      }
  //    //启动listener
  //    context
  //      .child("heartBeatsListener")
  //      .map {
  //        ref =>
  //          ref ! SyncControllerMessage("start")
  //      }
  //  }

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
        ref =>
          ref ! SyncControllerMessage("start")
          context.system.scheduler.schedule(SettingConstant.OFFSET_SAVE_CONSTANT seconds, SettingConstant.OFFSET_SAVE_CONSTANT seconds, ref, SyncControllerMessage("save"))
          context.system.scheduler.schedule(SettingConstant.CHECKSEND_CONSTANT seconds, SettingConstant.CHECKSEND_CONSTANT seconds, ref, SyncControllerMessage("checkSend"))
      }
    //启动batcher
    context
      .child("binlogBatcher")
      .map {
        ref => context.system.scheduler.scheduleOnce(SettingConstant.BATCHER_START_DELAY second, ref, akka.routing.Broadcast(SyncControllerMessage("start")))
      }
    context.child("ddlHandler")
      .map {
        ref => context.system.scheduler.scheduleOnce(SettingConstant.BATCHER_START_DELAY second, ref, (SyncControllerMessage("start")))

      }
    //启动fetcher
    context
      .child("binlogFetcher")
      .map {
        ref => context.system.scheduler.scheduleOnce(SettingConstant.FETCHER_START_DELAY second, ref, SyncControllerMessage("start"))
      }
    //启动listener
    context
      .child("heartBeatsListener")
      .map {
        ref =>
          ref ! SyncControllerMessage("start")
          //开始之后每`queryTimeOut`毫秒一次
          context.system.scheduler.schedule(SettingConstant.LISTEN_QUERY_TIMEOUT seconds, SettingConstant.LISTEN_QUERY_TIMEOUT seconds, ref, ListenerMessage("listen"))
      }

    if (taskInfoBean.isCosting)
      context
        .child("powerAdapter")
        .map(ref =>
          context
            .system
            .scheduler
            .schedule(SettingConstant.COMPUTE_COST_CONSTANT seconds, SettingConstant.COMPUTE_COST_CONSTANT seconds, ref, SyncControllerMessage("cost")));
    log.info("cost compute ON")
    if (taskInfoBean.isPowerAdapted) context
      .child("powerAdapter")
      .map(ref =>
        context.
          system
          .scheduler
          .schedule(SettingConstant.POWER_CONTROL_CONSTANT seconds, SettingConstant.POWER_CONTROL_CONSTANT seconds, ref, SyncControllerMessage("control")));
    log.info("power Control ON")
  }

  def initWorkers: Unit = {

    //初始化powerAdapter
    log.info("initialize powerAdapter")
    context.actorOf(PowerAdapter.props(mysql2KafkaTaskInfoManager), "powerAdapter")
    //初始化HeartBeatsListener
    log.info("initialize listener")
    context.actorOf(MysqlConnectionListener.props(mysql2KafkaTaskInfoManager).withDispatcher("akka.pinned-dispatcher"), "heartBeatsListener")
    //初始化binlogPositionRecorder
    //    log.info("initialize Recorder")
    //    val recorder = context.actorOf(MysqlBinlogPositionRecorder.props(mysql2KafkaTaskInfoManager), "binlogPositionRecorder")
    //初始化binlogSinker
    //如果并行打开使用并行sinker
    log.info("initialize sinker")
    val binlogSinker = if (resourceManager.taskInfo.isTransactional) {
      log.info("initialize sinker with mode transactional ")
      //使用transaction式
      context.actorOf(Props(classOf[BinlogTransactionBufferSinker], resourceManager), "binlogSinker")
    } else {
      log.info("initialize sinker with mode concurrent ")
      context.actorOf(ConcurrentBinlogSinker.prop(resourceManager), "binlogSinker")
    }
    log.info("initialize batcher")

    //    def initBatchersAndRouter:List[String] = {
    //      //batcher数量
    //      val num = taskInfoBean.batcherNum
    //      val pathPrefix = s"/user/syncDaemon/${taskInfoBean.syncTaskId}"
    //      //初始化ddl处理器
    //      context.actorOf(BinlogEventBatcher
    //        .prop(binlogSinker, resourceManager, true).withDispatcher(""), "ddlHandler")
    //      val paths = Range(0, num)
    //      //初始化batcher
    //        .map{
    //          index =>
    //            context.actorOf(BinlogEventBatcher
    //              .prop(binlogSinker, resourceManager).withDispatcher("akka.batcher-dispatcher"), s"binlogBatcher$index")
    //           s"$pathPrefix/binlogBatcher$index"
    //        }
    //       .toList
    //      paths
    //    }
    //
    //    //初始化binlogEventBatcher
    //    val binlogEventBatcher = context.actorOf( DivideDDLRoundRobinRoutingGroup(initBatchersAndRouter).props(),"binlogBatcher")
    val binlogDdlHandler = context.actorOf(BinlogEventBatcher.prop(binlogSinker, resourceManager, true).withDispatcher("akka.batcher-dispatcher"), "ddlHandler")
    val binlogEventBatcher = context.actorOf(BinlogEventBatcher
      .prop(binlogSinker, resourceManager)
      .withRouter(new RoundRobinPool(taskInfoBean.batcherNum).withDispatcher("akka.batcher-dispatcher").withSupervisorStrategy(OneForOneStrategy() {
        case _ => Escalate
      })), "binlogBatcher")
    log.info("initialize fetcher")
    //初始化binlogFetcher
    context.actorOf(MysqlBinlogFetcher.props(resourceManager, binlogEventBatcher, binlogDdlHandler).withDispatcher("akka.pinned-dispatcher"), "binlogFetcher")

  }

  /**
    * 错误处理
    */
  @deprecated
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {

    errorCount += 1
    if (isCrashed) {
      controllerChangeStatus(Status.ERROR)
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
  private def changeFunc(status: Status) = TaskManager.changeFunc(status, mysql2KafkaTaskInfoManager)

  private def onChangeFunc = Mysql2KafkaTaskInfoManager.onChangeStatus(mysql2KafkaTaskInfoManager)

  private def controllerChangeStatus(status: Status) = TaskManager.changeStatus(status, changeFunc, onChangeFunc)

  /**
    * **************** Actor生命周期 *******************
    */

  /**
    * 每次启动都会调用，在构造器之后调用
    * 0.初始化positionRecorder
    * 1.初始化HeartBeatsListener
    * 2.初始化binlogSinker
    * 3.初始化binlogEventBatcher
    * 4.初始化binlogFetcher
    */
  override def preStart(): Unit
  = {
    controllerChangeStatus(Status.OFFLINE)
    log.info("start init all workers")
    initWorkers
    mysql2KafkaTaskInfoManager.powerAdapter = context.child("powerAdapter")
  }

  //正常关闭时会调用，关闭资源
  override def postStop(): Unit

  = {
    log.info("syncController processing postStop ")

    //    mysqlConnection.disconnect()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit

  = {
    log.info("syncController processing preRestart")
    //默认的话是会调用postStop，preRestart可以保存当前状态s
    controllerChangeStatus(Status.RESTARTING)
    context.become(receive)
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit

  = {
    log.info("syncController processing postRestart")
    log.info(s"syncController will restart in ${SettingConstant.TASK_RESTART_DELAY} seconds")

    context.system.scheduler.scheduleOnce(SettingConstant.TASK_RESTART_DELAY seconds, self, SyncControllerMessage("restart"))
    //可以恢复之前的状态，默认会调用
    super.postRestart(reason)

  }

  override def supervisorStrategy = {
    AllForOneStrategy() {
      case e: ZkTimeoutException => {
        controllerChangeStatus(Status.ERROR)
        Escalate
      }
      case e: Exception => {
        controllerChangeStatus(Status.ERROR)
        Escalate

      }
      case error: Error => {
        controllerChangeStatus(Status.ERROR)
        Escalate
      }
      case _ => {
        controllerChangeStatus(Status.ERROR)
        Escalate
      }
    }
  }

  @deprecated
  def controllerRestartStrategy = {
    log.info("syncController will restart in 1 minute")
    context.system.scheduler.scheduleOnce(1 minute, self, SyncControllerMessage("restart"))
    Restart
  }
}

object MysqlBinlogController {
  def props(taskInfoBean: Mysql2KafkaTaskInfoBean): Props = {
    Props(new MysqlBinlogController(taskInfoBean))
  }
}

