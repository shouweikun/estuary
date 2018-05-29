package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import java.util.concurrent.Executors

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, AllForOneStrategy, Props}
import com.neighborhood.aka.laplace.estuary.bean.exception.control.{RestartCommandException, WorkerCannotFindException}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.{Status, SyncController}
import com.neighborhood.aka.laplace.estuary.core.lifecycle._
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.neighborhood.aka.laplace.estuary.mysql.task.{Mysql2KafkaTaskInfoBean, Mysql2KafkaTaskInfoManager}
import org.I0Itec.zkclient.exception.ZkTimeoutException

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Created by john_liu on 2018/2/1.
  */

class MysqlBinlogInOrderController(
                                    taskInfoBean: Mysql2KafkaTaskInfoBean
                                  ) extends SyncController with Actor with ActorLogging {

  lazy val schedulingCommandPool = Executors.newFixedThreadPool(3)
  /**
    * 必须要用这个，保证重启后，之前的定时发送任务都没了
    */
  implicit val scheduleTaskPool = ExecutionContext.fromExecutor(schedulingCommandPool)
  //资源管理器，一次同步任务所有的resource都由resourceManager负责
  val resourceManager = Mysql2KafkaTaskInfoManager.buildManager(taskInfoBean)
  val mysql2KafkaTaskInfoManager = resourceManager
  //canal的mysqlConnection
  val mysqlConnection: MysqlConnection = resourceManager.mysqlConnection
  val syncTaskId = taskInfoBean.syncTaskId
  val isCounting = taskInfoBean.isCounting
  val isCosting = taskInfoBean.isCosting
  val isPowerAdapted = taskInfoBean.isPowerAdapted


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

      log.info(s"controller switched to online,start all workers,id:$syncTaskId")
    }
    case "restart" => {
      context.become(online)
      controllerChangeStatus(Status.ONLINE)
      startAllWorkers
      log.info(s"controller switched to online,restart all workers,id:$syncTaskId")
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
    case "restart" => throw new RestartCommandException(
      {
        log.warning(s"restart sync task:$syncTaskId at ${System.currentTimeMillis()}");
        s"restart a,id:$syncTaskId"
      }
    )
    case ListenerMessage(msg) => {
      msg match {
        case x => {
          log.warning(s"syncController online unhandled message:$x,id:$syncTaskId")
        }
      }
    }
    case SinkerMessage(msg) => {
      msg match {
        case "error" => throw new RestartCommandException(
          {
            log.error(s"sinker went wrong when sending data,id:$syncTaskId");
            s"sinker went wrong when sending data,prepare to restart,id:$syncTaskId"
          }
        )
        case "flush" => {
          context
            .child("binlogBatcher")
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
        case x: Int => context.child("binlogFetcher").fold(throw new Exception(s"fetcher cannot be null,id:$syncTaskId"))(ref => ref ! SyncControllerMessage(x))
        case x: Long => context.child("binlogFetcher").fold(throw new Exception(s"fetcher cannot be null,id:$syncTaskId"))(ref => ref ! SyncControllerMessage(x))
        case _ => {}
      }
    }

  }

  def startAllWorkers = {
    //启动sinker
    context
      .child("binlogSinker")
      .fold {
        log.error(s"binlogSinker is null,id:$syncTaskId");
        throw new WorkerCannotFindException(s"binlogSinker is null,id:$syncTaskId")
      } {
        ref =>
          ref ! SyncControllerMessage("start")


          context.system.scheduler.schedule((SettingConstant.OFFSET_SAVE_CONSTANT + SettingConstant.COMPUTE_FIRST_DELAY) seconds, SettingConstant.OFFSET_SAVE_CONSTANT seconds, ref, SyncControllerMessage("save"))
      }
    //启动batcher
    context
      .child("binlogBatcher")
      .fold {
        log.error(s"binlogBatcher is null,id:$syncTaskId");
        throw new WorkerCannotFindException(s"binlogBatcher is null,id:$syncTaskId")
      } {
        ref =>
          context.system.scheduler.scheduleOnce(SettingConstant.BATCHER_START_DELAY second, ref, SyncControllerMessage("start"))
          context.system.scheduler.schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.CHECKSEND_CONSTANT seconds, ref, SyncControllerMessage("check"))
      }
    //启动fetcher
    context
      .child("binlogFetcher")
      .fold {
        log.error(s"binlogFetcher is null,id:$syncTaskId");
        throw new WorkerCannotFindException(s"binlogFetcher is null,id:$syncTaskId")
      } {
        ref => context.system.scheduler.scheduleOnce(SettingConstant.FETCHER_START_DELAY second, ref, SyncControllerMessage("start"))
      }
    //启动listener
    context
      .child("heartBeatsListener")
      .fold {
        log.error(s"heartBeatsListener is null,id:$syncTaskId");
        throw new WorkerCannotFindException(s"heartBeatsListener is null,id:$syncTaskId")
      } {
        ref =>
          ref ! SyncControllerMessage("start")
          //开始之后每`queryTimeOut`毫秒一次
          context.system.scheduler.schedule(SettingConstant.LISTEN_QUERY_TIMEOUT seconds, SettingConstant.LISTEN_QUERY_TIMEOUT seconds, ref, ListenerMessage("listen"))
      }

    if (isCosting)
      context
        .child("powerAdapter")
        .fold {
          log.error(s"powerAdapter is null,id:$syncTaskId");
          throw new WorkerCannotFindException(s"powerAdapter is null,id:$syncTaskId")
        }(ref =>
          context
            .system
            .scheduler
            .schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.COMPUTE_COST_CONSTANT seconds, ref, SyncControllerMessage("cost")));
    log.info(s"cost compute ON,id:$syncTaskId")
    if (isCounting)
      context
        .child("processingCounter")
        .fold {
          log.error(s"processingCounter is null,id:$syncTaskId");
          throw new WorkerCannotFindException(s"processingCounter is null,id:$syncTaskId")
        } {
          ref =>
            context
              .system
              .scheduler
              .schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.COMPUTE_COST_CONSTANT seconds, ref, SyncControllerMessage("count"));
            log.info(s"count compute ON,id:$syncTaskId")
        }
    if (isPowerAdapted) context
      .child("powerAdapter")
      .fold {
        log.error(s"powerAdapter is null,id:$syncTaskId");
        throw new WorkerCannotFindException(s"powerAdapter is null,id:$syncTaskId")
      }(ref =>
        context.
          system
          .scheduler
          .schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.POWER_CONTROL_CONSTANT seconds, ref, SyncControllerMessage("control")));
    log.info(s"power Control ON,id:$syncTaskId")
  }

  def initWorkers: Unit = {
    //初始化processingCounter
    log.info(s"initialize processingCounter,id:$syncTaskId")
    context.actorOf(MysqlInOrderProcessingCounter.props(mysql2KafkaTaskInfoManager), "processingCounter")
    //初始化powerAdapter
    log.info(s"initialize powerAdapter,id:$syncTaskId")
    context.actorOf(MysqlBinlogInOrderPowerAdapter.props(mysql2KafkaTaskInfoManager), "powerAdapter")
    //初始化HeartBeatsListener
    log.info(s"initialize heartBeatListener,id:$syncTaskId")
    context.actorOf(MysqlConnectionInOrderListener.props(mysql2KafkaTaskInfoManager).withDispatcher("akka.pinned-dispatcher")
      , "heartBeatsListener"
    )
    //初始化binlogPositionRecorder
    //    log.info("initialize Recorder")
    //    val recorder = context.actorOf(MysqlBinlogPositionRecorder.props(mysql2KafkaTaskInfoManager), "binlogPositionRecorder")
    //初始化binlogSinker
    //如果并行打开使用并行sinker
    log.info(s"initialize sinker,id:$syncTaskId")
    val binlogSinker = context.actorOf(MysqlBinlogInOrderSinkerManager.props(resourceManager), "binlogSinker")

    log.info(s"initialize batcher,id:$syncTaskId")
    val binlogEventBatcher = context.actorOf(MysqlBinlogInOrderBatcherManager
      .props(resourceManager, binlogSinker).withDispatcher("akka.batcher-dispatcher"), "binlogBatcher")
    log.info(s"initialize fetcher,id:$syncTaskId")
    //初始化binlogFetcher
    context.actorOf(MysqlBinlogInOrderFetcher.props(resourceManager, binlogEventBatcher).withDispatcher("akka.pinned-dispatcher"), "binlogFetcher")

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
    } else self ! message

  }

  /**
    * ********************* 状态变化 *******************
    */
  private def changeFunc(status: Status) = TaskManager.changeFunc(status, mysql2KafkaTaskInfoManager)

  private def onChangeFunc = TaskManager.onChangeStatus(mysql2KafkaTaskInfoManager)

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
    log.info(s"start init all workers,id:$syncTaskId")
    initWorkers
    mysql2KafkaTaskInfoManager.powerAdapter = context.child("powerAdapter")
    mysql2KafkaTaskInfoManager.processingCounter = context.child("processingCounter")
  }

  //正常关闭时会调用，关闭资源
  override def postStop(): Unit

  = {
    log.info(s"syncController processing postStop ,id:$syncTaskId")
    schedulingCommandPool.shutdown()
    //    mysqlConnection.disconnect()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit

  = {
    log.info(s"syncController processing preRestart,id:$syncTaskId")
    //默认的话是会调用postStop，preRestart可以保存当前状态s
    controllerChangeStatus(Status.RESTARTING)
    context.become(receive)
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit

  = {
    log.info(s"syncController processing postRestart,id:$syncTaskId")
    log.info(s"syncController will restart in ${SettingConstant.TASK_RESTART_DELAY} seconds,id:$syncTaskId")

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

}

object MysqlBinlogInOrderController {
  def props(taskInfoBean: Mysql2KafkaTaskInfoBean): Props = Props(new MysqlBinlogInOrderController(taskInfoBean))
}



