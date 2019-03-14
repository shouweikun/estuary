package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.control

import java.util.concurrent.{ExecutorService, Executors}

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{ActorRef, AllForOneStrategy, Props}
import com.neighborhood.aka.laplace.estuary.bean.datasink.DataSinkBean
import com.neighborhood.aka.laplace.estuary.bean.exception.control.WorkerCannotFindException
import com.neighborhood.aka.laplace.estuary.bean.exception.other.WorkerInitialFailureException
import com.neighborhood.aka.laplace.estuary.bean.identity.BaseExtractBean
import com.neighborhood.aka.laplace.estuary.bean.resource.MysqlSourceBean
import com.neighborhood.aka.laplace.estuary.core.akkaUtil.SyncDaemonCommand.{ExternalRestartCommand, ExternalStartCommand}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle._
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SyncControllerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.adapt.MysqlBinlogInOrderPowerAdapterCommand.{MysqlBinlogInOrderPowerAdapterComputeCost, MysqlBinlogInOrderPowerAdapterControl, MysqlBinlogInOrderPowerAdapterDelayFetch}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderBatcherCommand
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderBatcherCommand.MysqlBinlogInOrderBatcherCheckHeartbeats
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.control.MysqlBinlogInOrderControllerCommand._
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.count.MysqlBinlogInOrderProcessingCounterCommand.MysqlBinlogInOrderProcessingCounterComputeCount
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch.MysqlBinlogInOrderFetcherCommand
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.listen.MysqlBinlogInOrderListenerCommand
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.listen.MysqlBinlogInOrderListenerCommand.MysqlBinlogInOrderListenerListen
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.record.MysqlBinlogInOrderRecorderCommand.MysqlBinlogInOrderRecorderSavePosition
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinkerCommand
import com.neighborhood.aka.laplace.estuary.mysql.source.{MysqlConnection, MysqlSourceManagerImp}
import com.neighborhood.aka.laplace.estuary.mysql.task.mysql.Mysql2MysqlAllTaskInfoBean
import org.I0Itec.zkclient.exception.ZkTimeoutException

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Try

/**
  * Created by john_liu on 2018/2/1.
  *
  * @tparam B SinkFunc
  * @author neighborhood.aka.laplace
  */

abstract class MysqlBinlogInOrderController[B <: SinkFunc](override val taskBean: BaseExtractBean,
                                                           override val sourceBean: MysqlSourceBean,
                                                           override val sinkBean: DataSinkBean[B]) extends SyncControllerPrototype[MysqlConnection, B] {

  protected val schedulingCommandPool: ExecutorService = Executors.newFixedThreadPool(3)
  /**
    * 必须要用这个，保证重启后，之前的定时发送任务都没了
    */
  implicit protected val scheduleTaskPool: ExecutionContextExecutor = ExecutionContext.fromExecutor(schedulingCommandPool)

  /**
    * 资源管理器，一次同步任务所有的resource都由resourceManager负责
    */
  override def resourceManager: MysqlSourceManagerImp with SinkManager[B] with TaskManager = buildManager.asInstanceOf[MysqlSourceManagerImp with SinkManager[B] with TaskManager]

  /**
    * 任务管理器
    */
  def taskManager: TaskManager = resourceManager

  /**
    * 数据库连接
    */
  val mysqlConnection: MysqlConnection = resourceManager.source
  /**
    * 同步任务标识
    */
  override val syncTaskId: String = taskManager.syncTaskId
  /**
    * 是否计数
    */
  val isCounting: Boolean = taskManager.isCounting
  /**
    * 是否计算耗时
    */
  val isCosting: Boolean = taskManager.isCosting
  /**
    * 是否进行功率控制
    */
  val isPowerAdapted: Boolean = taskManager.isPowerAdapted


  override def errorCountThreshold: Int = 3

  override var errorCount: Int = 0


  //offline 状态
  /**
    * 1. "start" => 切换为开始模式
    * 2. "restart" => 切换为开始模式,语义为`restart`
    * 3. 未使用
    * 4. SyncControllerMessage("restart") => 发起重启
    *
    * @return
    */
  override def receive: Receive = {
    case ExternalStartCommand => switch2Online
    case ExternalRestartCommand(`syncTaskId`) => restartBySupervisor
    case MysqlBinlogInOrderControllerStart => switch2Online
    case SyncControllerMessage(MysqlBinlogInOrderControllerStart) => switch2Online
    case ListenerMessage(MysqlBinlogInOrderControllerRestart) => sender ! MysqlBinlogInOrderControllerRestart
      (MysqlBinlogInOrderControllerStart)
    case SyncControllerMessage(MysqlBinlogInOrderControllerRestart) => self ! MysqlBinlogInOrderControllerRestart
    case MysqlBinlogInOrderControllerRestart =>
      log.info(s"controller switched to online,restart all workers,id:$syncTaskId")
      switch2Online
  }

  /**
    * 1. MysqlBinlogInOrderControllerStopAndRestart => 抛出RestartCommandException，基于监督机制重启
    * 2. ListenerMessage(msg) =>无用
    * 3. SinkerMessage(msg) => 无用
    * 4. SyncControllerMessage(MysqlBinlogInOrderControllerCheckRunningInfo) => 触发checkInfo方法
    * 5. SyncControllerMessage(x: Int) => 发送延迟给fetcher
    * 6. SyncControllerMessage(x: Long) => 同上
    * 7. SyncControllerMessage(msg) => 无用
    *
    * @return
    */
  override def online: Receive = {
    case ExternalRestartCommand(`syncTaskId`) => restartBySupervisor
    case MysqlBinlogInOrderControllerStopAndRestart => restartBySupervisor
    case ListenerMessage(msg) => log.warning(s"syncController online unhandled message:$msg,id:$syncTaskId")
    case SinkerMessage(msg) => log.warning(s"syncController online unhandled message:${SinkerMessage(msg)},id:$syncTaskId")
    case SyncControllerMessage(MysqlBinlogInOrderControllerCheckRunningInfo) => checkInfo
    case PowerAdapterMessage(x: MysqlBinlogInOrderPowerAdapterDelayFetch) => sendFetchDelay(x)
    case msg => log.warning(s"syncController online unhandled message:${msg},id:$syncTaskId")
  }


  /**
    * 发送fetch delay给fetcher
    *
    * @param x
    */
  protected def sendFetchDelay(x: MysqlBinlogInOrderPowerAdapterDelayFetch) = {
    context.child(fetcherName).fold(throw new WorkerCannotFindException(s"fetcher cannot be null,id:$syncTaskId"))(ref => ref ! SyncControllerMessage(x))
  }


  /**
    * 0. 发送定时打印任务运行信息的命令(checkInfo)
    * 1. 启动binlogSinker:       1.发送开始命令
    * 2. 启动binlogBatcher:      1.发送开始命令 2.发送定时心跳数据命令
    * 3. 启动binlogFetcher:      1.发送开始命令
    * 4. 启动heartbeatListener:  1.发送开始命令 2.发送定时监听数据源心跳命令
    * 5. 如果计时    发送计时命令给powerAdapter
    * 6. 如果计数    发送计数命令给countProcesser
    * 7. 如果功率控制 发送功率控制命令给powerAdapter
    * 8. 启动positionRecorder    1.发送定时保存offset命令
    */
  protected def startAllWorkers: Unit = {


    scheduleCheckInfoCommand(self)
    //启动sinker
    context
      .child(sinkerName)
      .fold {
        log.error(s"$sinkerName is null,id:$syncTaskId")
        throw new WorkerCannotFindException(s"$sinkerName is null,id:$syncTaskId")
      } {
        ref =>
          log.info(s"start $sinkerName,id:$syncTaskId")
          sendStartCommand(ref, sinkerName)

      }
    //启动batcher
    context
      .child(batcherName)
      .fold {
        log.error(s"$batcherName is null,id:$syncTaskId")
        throw new WorkerCannotFindException(s"$batcherName is null,id:$syncTaskId")
      } {
        ref =>
          log.info(s"start $batcherName in ${SettingConstant.BATCHER_START_DELAY},id:$syncTaskId")
          sendStartCommand(ref, batcherName, SettingConstant.BATCHER_START_DELAY)
          sendCheckHeartBeatsCommand(ref)
      }
    //启动fetcher
    context
      .child(fetcherName)
      .fold {
        log.error(s"$batcherName is null,id:$syncTaskId")
        throw new WorkerCannotFindException(s"$batcherName is null,id:$syncTaskId")
      } { ref =>
        log.info(s"start $batcherName in ${SettingConstant.FETCHER_START_DELAY},id:$syncTaskId")
        sendStartCommand(ref, fetcherName, SettingConstant.FETCHER_START_DELAY)
      }
    //启动listener
    context
      .child(listenerName)
      .fold {
        log.error(s"$listenerName is null,id:$syncTaskId")
        throw new WorkerCannotFindException(s"$listenerName is null,id:$syncTaskId")
      } {
        ref =>
          log.info(s"start $listenerName,id:$syncTaskId")
          sendStartCommand(ref, listenerName)
          //开始之后每`queryTimeOut`毫秒一次
          sendListenConnectionCommand(ref)
      }

    if (isCosting)
      context
        .child(powerAdapterName)
        .fold {
          log.error(s"$powerAdapterName is null,id:$syncTaskId")
          throw new WorkerCannotFindException(s"$powerAdapterName is null,id:$syncTaskId")
        }(ref => sendComputeCostMessage(ref))
    log.info(s"cost compute ON,id:$syncTaskId")
    if (isCounting)
      context
        .child(processingCounterName)
        .fold {
          log.error(s"$processingCounterName is null,id:$syncTaskId")
          throw new WorkerCannotFindException(s"$processingCounterName is null,id:$syncTaskId")
        } {
          ref =>
            sendComputeCountMessage(ref)
            log.info(s"count compute ON,id:$syncTaskId")
        }
    if (isPowerAdapted) context
      .child(powerAdapterName)
      .fold {
        log.error(s"$powerAdapterName is null,id:$syncTaskId")
        throw new WorkerCannotFindException(s"$powerAdapterName is null,id:$syncTaskId")
      }(ref => sendPowerControlMessage(ref))
    log.info(s"power Control ON,id:$syncTaskId")

    //启动positionRecorder
    context
      .child(positionRecorderName)
      .fold {
        log.error(s"$positionRecorderName is null,id:$syncTaskId")
        throw new WorkerCannotFindException(s"$positionRecorderName is null,id:$syncTaskId")
      } { ref => scheduleSaveCommand(ref) }
  }

  /**
    * 发送启动命令
    * 支持延迟发送
    */
  private def sendStartCommand(ref: ActorRef, workerName: String, delay: Int = 0): Unit = {
    lazy val message = workerName match {
      case `sinkerName` => MysqlBinlogInOrderSinkerCommand.MysqlInOrderSinkerStart
      case `fetcherName` => MysqlBinlogInOrderFetcherCommand.MysqlBinlogInOrderFetcherStart
      case `batcherName` => MysqlBinlogInOrderBatcherCommand.MysqlBinlogInOrderBatcherStart
      case `listenerName` => MysqlBinlogInOrderListenerCommand.MysqlBinlogInOrderListenerStart
    }
    lazy val finalMessage = SyncControllerMessage(message)
    delay match {
      case x if (x > 0) => context.system.scheduler.scheduleOnce(delay second, ref, finalMessage)
      case 0 => ref ! finalMessage
      case _ => throw new IllegalArgumentException(s"delay can not be less than 0,id:$syncTaskId")
    }

  }

  /**
    * 发送检查程序运行状态的定时指令
    * 固定日志输出
    *
    * @param ref
    */
  private def scheduleCheckInfoCommand(ref: ActorRef = self): Unit = {
    log.info(s"schedule `checkInfo` message per ${SettingConstant.CHECK_STATUS_INTERVAL} seconds,id:$syncTaskId")
    context.system.scheduler.schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.CHECK_STATUS_INTERVAL seconds, ref, SyncControllerMessage(MysqlBinlogInOrderControllerCheckRunningInfo))
  }

  /**
    * 发送保存任务当前binlog offset的指令
    *
    * @param ref ActorRef
    */
  private def scheduleSaveCommand(ref: ActorRef): Unit = {
    log.info(s"schedule `save` message per ${SettingConstant.OFFSET_SAVE_CONSTANT} seconds,id:$syncTaskId")
    context.system.scheduler.schedule((SettingConstant.OFFSET_SAVE_CONSTANT + SettingConstant.COMPUTE_FIRST_DELAY) seconds, SettingConstant.OFFSET_SAVE_CONSTANT seconds, ref, SyncControllerMessage(MysqlBinlogInOrderRecorderSavePosition))
  }

  /**
    * 发送定时心跳数据的指令
    *
    * @param ref ActorRef
    */
  private def sendCheckHeartBeatsCommand(ref: ActorRef): Unit = {
    log.info(s"schedule `check` message per ${SettingConstant.CHECKSEND_CONSTANT} seconds,id:$syncTaskId")
    context.system.scheduler.schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.CHECKSEND_CONSTANT seconds, ref, SyncControllerMessage(MysqlBinlogInOrderBatcherCheckHeartbeats)
    )

  }

  /**
    * 发送心跳监听的指令
    *
    * @param ref ActorRef
    */
  private def sendListenConnectionCommand(ref: ActorRef): Unit = {
    log.info(s"schedule `MysqlBinlogInOrderListenerListen` message per ${SettingConstant.LISTEN_QUERY_TIMEOUT} seconds,id:$syncTaskId")
    context.system.scheduler.schedule(SettingConstant.LISTEN_QUERY_TIMEOUT seconds, SettingConstant.LISTEN_QUERY_TIMEOUT seconds, ref, SyncControllerMessage(MysqlBinlogInOrderListenerListen))
  }

  /**
    * 发送计算耗时的指令
    *
    * @param ref ActorRef
    */
  private def sendComputeCostMessage(ref: ActorRef): Unit = {
    log.info(s"schedule `cost` message per ${SettingConstant.COMPUTE_COST_CONSTANT} seconds,id:$syncTaskId")
    context.system.scheduler.schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.COMPUTE_COST_CONSTANT seconds, ref, SyncControllerMessage(MysqlBinlogInOrderPowerAdapterComputeCost))
  }

  /**
    * 发送计数的指令
    *
    * @param ref ActorRef
    */
  private def sendComputeCountMessage(ref: ActorRef): Unit = {
    log.info(s"schedule `count` message per ${SettingConstant.COMPUTE_COST_CONSTANT} seconds,id:$syncTaskId")
    context.system.scheduler.schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.COMPUTE_COUNT_CONSTANT milliseconds, ref, SyncControllerMessage(MysqlBinlogInOrderProcessingCounterComputeCount))
  }

  /**
    * 发送功率控制指令
    *
    * @param ref ActorRef
    */
  private def sendPowerControlMessage(ref: ActorRef): Unit = {
    log.info(s"schedule `control` message per ${SettingConstant.POWER_CONTROL_CONSTANT} milliseconds,id:$syncTaskId")
    context.system.scheduler.schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.POWER_CONTROL_CONSTANT milliseconds, ref, SyncControllerMessage(MysqlBinlogInOrderPowerAdapterControl))
  }

  /**
    * 1.初始化HeartBeatsListener
    * 2.初始化binlogSinker
    * 3.初始化binlogEventBatcher
    * 4.初始化binlogFetcher
    * 5.初始化processingCounter
    * 6.初始化powerAdapter
    * 7.初始化positionRecorder
    */
  def initWorkers: Unit

  //  /**
  //    * 下发snapshot任务
  //    *
  //    * @param task snapshot任务
  //    */
  //  private def dispatchSnapshotTask(task: Mysql2KafkaSnapshotTask): Unit = {
  //    log.info(s"dispatch SnapshotTask:$task,id:$syncTaskId")
  //    context
  //      .child("binlogFetcher")
  //      .fold {
  //        log.error(s"binlogFetcher is null,id:$syncTaskId")
  //        throw new WorkerCannotFindException(s"binlogFetcher is null,id:$syncTaskId")
  //      } { ref => ref ! SyncControllerMessage(task) }
  //  }

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
    taskManager.start
    TaskManager.putTaskManager(syncTaskId, taskManager)
  }

  //正常关闭时会调用，关闭资源
  override def postStop(): Unit

  = {
    log.info(s"syncController processing postStop ,id:$syncTaskId")
    if (!schedulingCommandPool.isShutdown) Try(schedulingCommandPool.shutdownNow())
    TaskManager.removeTaskManager(syncTaskId) // 这步很必要
    taskManager.close
    if (!resourceManager.sink.isTerminated) resourceManager.sink.close
    if (resourceManager.source.isConnected) resourceManager.source.disconnect()

  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit

  = {
    log.info(s"syncController processing preRestart,id:$syncTaskId")
    //默认的话是会调用postStop，preRestart可以保存当前状态s
    controllerChangeStatus(Status.RESTARTING)
    context.become(receive)
    super.preRestart(reason, message)
    taskManager.close
    log.info(s"syncController processing preRestart complete,id:$syncTaskId")
  }

  override def postRestart(reason: Throwable): Unit

  = {
    log.info(s"syncController processing postRestart,id:$syncTaskId")
    log.info(s"syncController will restart in ${SettingConstant.TASK_RESTART_DELAY} seconds,id:$syncTaskId")

    context.system.scheduler.scheduleOnce(SettingConstant.TASK_RESTART_DELAY seconds, self, SyncControllerMessage(MysqlBinlogInOrderControllerStart))
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
  /**
    * 工厂方法
    *
    * @param taskInfoBean 任务信息Bean
    * @param name         加载类型名称
    * @return 构建好的控制器
    */
  def buildMysqlBinlogInOrderController(taskInfoBean: BaseExtractBean, name: String): Props = {
    name match {
      case MysqlBinlogInOrderMysqlController.name => MysqlBinlogInOrderMysqlController.props(taskInfoBean.asInstanceOf[Mysql2MysqlAllTaskInfoBean])
      case _ => throw new WorkerInitialFailureException(s"cannot build MysqlBinlogInOrderController name item match $name")
    }
  }
}



