package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.control.hdfs

import java.util.concurrent.{ExecutorService, Executors}

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{ActorRef, AllForOneStrategy}
import com.neighborhood.aka.laplace.estuary.bean.exception.control.WorkerCannotFindException
import com.neighborhood.aka.laplace.estuary.bean.exception.fetch.FetcherTimeoutException
import com.neighborhood.aka.laplace.estuary.core.akkaUtil.SyncDaemonCommand._
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SyncControllerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{ListenerMessage, PowerAdapterMessage, SinkerMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.sink.hdfs.HdfsSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mongo.SettingConstant
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt.OplogPowerAdapter
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt.OplogPowerAdapterCommand.{OplogPowerAdapterComputeCost, OplogPowerAdapterControl, OplogPowerAdapterDelayFetch}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.OplogBatcherCommand
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.OplogBatcherCommand.OplogBatcherCheckHeartbeats
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.control.OplogControllerCommand._
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.count.OplogProcessingCounter
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.count.OplogProcessingCounterCommand.OplogProcessingCounterComputeCount
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch.OplogFetcherCommand.OplogFetcherSuspend
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch.{OplogFetcherCommand, OplogFetcherManager}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.record.OplogPositionRecorder
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.record.OplogRecorderCommand.OplogRecorderSavePosition
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.OplogSinkerCommand
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.OplogSinkerCommand.{OplogSinkerCheckFlush, OplogSinkerCollectOffset, OplogSinkerSendOffset}
import com.neighborhood.aka.laplace.estuary.mongo.sink.hdfs.HdfsBeanImp
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoSourceBeanImp}
import com.neighborhood.aka.laplace.estuary.mongo.task.hdfs.{Mongo2HdfsAllTaskInfoBean, Mongo2HdfsTaskInfoBeanImp, Mongo2HdfsTaskInfoManager}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Try

/**
  * Created by john_liu on 2019/3/6.
  *
  * mongo2Hdfs的控制类
  *
  * @author neighborhood.aka.lapalce
  */
final class Oplog2HdfsController(
                                  val allTaskInfoBean: Mongo2HdfsAllTaskInfoBean
                                ) extends SyncControllerPrototype[MongoConnection, HdfsSinkFunc] {

  protected val schedulingCommandPool: ExecutorService = Executors.newFixedThreadPool(3)
  /**
    * 必须要用这个，保证重启后，之前的定时发送任务都没了
    */
  implicit protected val scheduleTaskPool: ExecutionContextExecutor = ExecutionContext.fromExecutor(schedulingCommandPool)
  override val taskBean: Mongo2HdfsTaskInfoBeanImp = allTaskInfoBean.taskRunningInfoBean
  override val sourceBean: MongoSourceBeanImp = allTaskInfoBean.sourceBean

  override val sinkBean: HdfsBeanImp = allTaskInfoBean.sinkBean

  val logIsEnabled = taskBean.logEnabled
  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskBean.syncTaskId

  /**
    * 任务信息管理器
    */
  override val taskManager: Mongo2HdfsTaskInfoManager = buildManager


  val isCosting = taskManager.isCosting
  val isCounting = taskManager.isCounting
  val isPowerAdapted = taskManager.isPowerAdapted

  if (logIsEnabled) log.info(s"Oplog2HdfsController start build,id:$syncTaskId")

  override def resourceManager: Mongo2HdfsTaskInfoManager = taskManager

  override def receive: Receive = {
    case ExternalStartCommand => switch2Online
    case ExternalRestartCommand(`syncTaskId`) => restartBySupervisor
    case OplogControllerStart => switch2Online
    case SyncControllerMessage(OplogControllerStart) => switch2Online
      (OplogControllerStart)
    case SyncControllerMessage(OplogControllerRestart) => self ! OplogControllerRestart
    case OplogControllerRestart =>
      log.info(s"controller switched to online,restart all workers,id:$syncTaskId")
      switch2Online
  }

  /**
    * 1. OplogControllerStopAndRestart => 抛出RestartCommandException，基于监督机制重启
    * 2. ListenerMessage(msg) =>无用
    * 3. SinkerMessage(msg) => 无用
    * 4. SyncControllerMessage(OplogControllerCheckRunningInfo) => 触发checkInfo方法
    * 5. SyncControllerMessage(x: Int) => 发送延迟给fetcher
    * 6. SyncControllerMessage(x: Long) => 同上
    * 7. SyncControllerMessage(msg) => 无用
    *
    * @return
    */
  override def online: Receive = {
    case ExternalRestartCommand(`syncTaskId`) => restartBySupervisor
    case ExternalRestartCommand(x) if (syncTaskId.contains(x)) => restartBySupervisor
    case ExternalSuspendCommand(_) => suspendFetcher
    case ExternalResumeCommand(_) => resumeFetcher
    case m@ExternalSuspendTimedCommand(_, x) => handleTimedSuspend(x)
    case OplogControllerStopAndRestart => restartBySupervisor
    case OplogFetcherSuspend => suspendFetcher
    case ListenerMessage(msg) => log.warning(s"syncController online unhandled message:$msg,id:$syncTaskId")
    case SinkerMessage(msg) => log.warning(s"syncController online unhandled message:${SinkerMessage(msg)},id:$syncTaskId")
    case SyncControllerMessage(OplogControllerCheckRunningInfo) => checkInfo
    case PowerAdapterMessage(x: OplogPowerAdapterDelayFetch) => sendFetchDelay(x)
    case msg => log.warning(s"syncController online unhandled message:${msg},id:$syncTaskId")
  }


  /**
    * 利用监督机制重启
    */
  override protected def restartBySupervisor = {
    handleTimedSuspend(-1) //确保调用重启以后，ts重新初始化
    super.restartBySupervisor
  }

  def handleTimedSuspend(ts: Long): Unit = {
    taskManager.fetchSuspendTs.set(ts)
    log.info(s"update suspendTs to:$ts,id:$syncTaskId")
  }

  /** *
    * 挂起fetcher
    */
  def suspendFetcher: Unit = {
    log.info(s"start suspend fetcher,id:$syncTaskId")
    context.child(fetcherName).foreach(context.stop(_))
    taskManager.fetcherStatus.set(Status.SUSPEND)
  }

  /**
    * 唤醒fetcher
    */
  def resumeFetcher: Unit = {
    log.info(s"resume fetcher,id:$syncTaskId")
    initFetcher(context.child(batcherName).get)
    startFetcher
  }

  /**
    * 任务资源管理器的构造的工厂方法
    *
    * @return 构造好的资源管理器
    *
    */
  override def buildManager: Mongo2HdfsTaskInfoManager = {
    log.info(s"start to build Mongo2HdfsTaskInfoManager,id:$syncTaskId")
    new Mongo2HdfsTaskInfoManager(allTaskInfoBean, context.system.settings.config)
  }

  /**
    * 初始化workers
    * ------1.初始化HeartBeatsListener
    * 2.初始化binlogSinker
    * 3.初始化binlogEventBatcher
    * 4.初始化binlogFetcher
    * 5.初始化processingCounter
    * 6.初始化powerAdapter
    * 7.初始化positionRecorder
    */
  override def initWorkers: Unit = {
    log.info(s"Oplog2HdfsController start init workers,id:$syncTaskId")
    //初始化processingCounter
    log.info(s"initialize processingCounter,id:$syncTaskId")
    val processingCounter = context.actorOf(OplogProcessingCounter.props(taskManager), processingCounterName)
    taskManager.processingCounter = Option(processingCounter) //必须要processingCounter嵌入taskManager
    //初始化powerAdapter
    log.info(s"initialize powerAdapter,id:$syncTaskId")
    val powerAdapter = context.actorOf(OplogPowerAdapter.buildOplogPowerAdapterByName(taskManager, ""), powerAdapterName)
    taskManager.powerAdapter = Option(powerAdapter) //必须要powerAdapter嵌入taskManager

    // 初始化binlogPositionRecorder
    log.info(s"initialize Recorder,id:$syncTaskId")
    val recorder = context.actorOf(OplogPositionRecorder.props(taskManager), positionRecorderName)
    taskManager.positionRecorder = Option(recorder) //必须要将PositionRecorder嵌入taskManager


    //初始化HeartBeatsListener
    //    log.info(s"initialize heartBeatListener,id:$syncTaskId")
    //    context.actorOf(MysqlConnectionInOrderListener.props(resourceManager).withDispatcher("akka.pinned-dispatcher"), listenerName)

    //初始化binlogSinker
    log.info(s"initialize sinker,id:$syncTaskId")
    //todo

    taskManager.wait4SinkerList() //必须要等待
    //初始化batcher
    log.info(s"initialize batcher,id:$syncTaskId")
    val oplogBatcher = ??? //todo
    initFetcher(oplogBatcher)
  }

  protected def initFetcher(batcher: ActorRef): ActorRef = {
    // 初始化binlogFetcher
    log.info(s"initialize fetcher,id:$syncTaskId")
    context.actorOf(OplogFetcherManager.props(resourceManager, batcher).withDispatcher("akka.fetcher-dispatcher"), fetcherName)
  }


  /**
    * 0. 发送定时打印任务运行信息的命令(checkInfo)
    * 1. 启动binlogSinker:       1.发送开始命令
    * 2. 启动binlogBatcher:      1.发送开始命令 2.发送定时心跳数据命令
    * 3. 启动binlogFetcher:      1.发送开始命令
    * ---------------- 4. 启动heartbeatListener:  1.发送开始命令 2.发送定时监听数据源心跳命令
    * 5. 如果计时    发送计时命令给powerAdapter
    * 6. 如果计数    发送计数命令给countProcesser
    * 7. 如果功率控制 发送功率控制命令给powerAdapter
    * 8. 启动positionRecorder    1.发送定时保存offset命令
    */
  protected def startAllWorkers: Unit = {

    if (taskBean.logEnabled) scheduleCheckInfoCommand(self)
    //启动sinker
    startSinker
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

    startFetcher

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

  protected def startFetcher: Unit = {
    //启动fetcher
    context
      .child(fetcherName)
      .fold {
        log.error(s"$fetcherName is null,id:$syncTaskId")
        throw new WorkerCannotFindException(s"$batcherName is null,id:$syncTaskId")
      } { ref =>
        log.info(s"start $fetcherName in ${SettingConstant.FETCHER_START_DELAY},id:$syncTaskId")
        sendStartCommand(ref, fetcherName, SettingConstant.FETCHER_START_DELAY)
      }
  }

  /**
    * 1.开始命令
    * 2.flush
    * 3.send offset
    * 4.collect offset
    */
  def startSinker: Unit = {
    context
      .child(sinkerName)
      .fold {
        log.error(s"$sinkerName is null,id:$syncTaskId")
        throw new WorkerCannotFindException(s"$sinkerName is null,id:$syncTaskId")
      } {
        ref =>
          log.info(s"start $sinkerName,id:$syncTaskId")
          sendStartCommand(ref, sinkerName)
          sendCheckHdfsFlushCommand(ref)
          sendCheckSendOffsetCommand(ref)
          sendCheckCollectOffsetCommand(ref)
      }
  }

  /**
    * 发送fetch delay给fetcher
    *
    * @param x
    */
  protected def sendFetchDelay(x: OplogPowerAdapterDelayFetch) = {
    context.child(fetcherName).fold(throw new WorkerCannotFindException(s"fetcher cannot be null,id:$syncTaskId"))(ref => ref ! SyncControllerMessage(x))
  }

  /**
    * 发送启动命令
    * 支持延迟发送
    */
  private def sendStartCommand(ref: ActorRef, workerName: String, delay: Int = 0): Unit = {
    lazy val message = workerName match {
      case `sinkerName` => OplogSinkerCommand.OplogSinkerStart
      case `fetcherName` => OplogFetcherCommand.OplogFetcherStart
      case `batcherName` => OplogBatcherCommand.OplogBatcherStart
      //      case `listenerName` => OplogListenerCommand.OplogListenerStart
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
    context.system.scheduler.schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.CHECK_STATUS_INTERVAL seconds, ref, SyncControllerMessage(OplogControllerCheckRunningInfo))
  }

  /**
    * 发送保存任务当前binlog offset的指令
    *
    * @param ref ActorRef
    */
  private def scheduleSaveCommand(ref: ActorRef): Unit = {
    log.info(s"schedule `save` message per ${SettingConstant.OFFSET_SAVE_CONSTANT} seconds,id:$syncTaskId")
    context.system.scheduler.schedule((SettingConstant.OFFSET_SAVE_CONSTANT + SettingConstant.COMPUTE_FIRST_DELAY) seconds, SettingConstant.OFFSET_SAVE_CONSTANT seconds, ref, SyncControllerMessage(OplogRecorderSavePosition))
  }

  /**
    * 发送定时心跳数据的指令
    *
    * @param ref ActorRef
    */
  private def sendCheckHeartBeatsCommand(ref: ActorRef): Unit = {
    log.info(s"schedule `check` message per ${SettingConstant.CHECKSEND_CONSTANT} seconds,id:$syncTaskId")
    context.system.scheduler.schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.CHECKSEND_CONSTANT seconds, ref, SyncControllerMessage(OplogBatcherCheckHeartbeats)
    )

  }

  private def sendCheckHdfsFlushCommand(ref: ActorRef): Unit = {
    log.info(s" schedule `OplogSinkerCheckFlush` message per ${SettingConstant.CHECK_HDFS_FLUSH_INTERVAL},id:$syncTaskId")
    context.system.scheduler.schedule(SettingConstant.CHECK_HDFS_FLUSH_INTERVAL * 2 + 2 seconds, SettingConstant.CHECK_HDFS_FLUSH_INTERVAL seconds, ref, SyncControllerMessage(OplogSinkerCheckFlush)
    )
  }

  private def sendCheckSendOffsetCommand(ref: ActorRef): Unit = {
    log.info(s" schedule `OplogSinkerSendOffset` message per ${SettingConstant.CHECK_SEND_OFFSET_INTERVAL},id:$syncTaskId")
    context.system.scheduler.schedule(SettingConstant.CHECK_SEND_OFFSET_INTERVAL * 2 + 3 seconds, SettingConstant.CHECK_SEND_OFFSET_INTERVAL seconds, ref, SyncControllerMessage(OplogSinkerSendOffset)
    )
  }

  private def sendCheckCollectOffsetCommand(ref: ActorRef): Unit = {
    log.info(s" schedule `OplogSinkerCollectOffset` message per ${SettingConstant.CHECK_COLLECT_OFFSET_INTERVAL},id:$syncTaskId")
    context.system.scheduler.schedule(SettingConstant.CHECK_COLLECT_OFFSET_INTERVAL * 2 + 1 seconds, SettingConstant.CHECK_COLLECT_OFFSET_INTERVAL seconds, ref, SyncControllerMessage(OplogSinkerCollectOffset)
    )
  }


  //  /**
  //    * 发送心跳监听的指令
  //    *
  //    * @param ref ActorRef
  //    */
  //  private def sendListenConnectionCommand(ref: ActorRef): Unit = {
  //    log.info(s"schedule `OplogListenerListen` message per ${SettingConstant.LISTEN_QUERY_TIMEOUT} seconds,id:$syncTaskId")
  //    context.system.scheduler.schedule(SettingConstant.LISTEN_QUERY_TIMEOUT seconds, SettingConstant.LISTEN_QUERY_TIMEOUT seconds, ref, SyncControllerMessage(OplogListenerListen))
  //  }

  /**
    * 发送计算耗时的指令
    *
    * @param ref ActorRef
    */
  private def sendComputeCostMessage(ref: ActorRef): Unit = {
    log.info(s"schedule `cost` message per ${SettingConstant.COMPUTE_COST_CONSTANT} seconds,id:$syncTaskId")
    context.system.scheduler.schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.COMPUTE_COST_CONSTANT seconds, ref, SyncControllerMessage(OplogPowerAdapterComputeCost))
  }

  /**
    * 发送计数的指令
    *
    * @param ref ActorRef
    */
  private def sendComputeCountMessage(ref: ActorRef): Unit = {
    log.info(s"schedule `count` message per ${SettingConstant.COMPUTE_COST_CONSTANT} seconds,id:$syncTaskId")
    context.system.scheduler.schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.COMPUTE_COUNT_CONSTANT milliseconds, ref, SyncControllerMessage(OplogProcessingCounterComputeCount))
  }

  /**
    * 发送功率控制指令
    *
    * @param ref ActorRef
    */
  private def sendPowerControlMessage(ref: ActorRef): Unit = {
    log.info(s"schedule `control` message per ${SettingConstant.POWER_CONTROL_CONSTANT} milliseconds,id:$syncTaskId")
    context.system.scheduler.schedule(SettingConstant.COMPUTE_FIRST_DELAY seconds, SettingConstant.POWER_CONTROL_CONSTANT milliseconds, ref, SyncControllerMessage(OplogPowerAdapterControl))
  }

  /**
    * 错位次数阈值
    */
  override def errorCountThreshold: Int = 0

  /**
    * 错位次数
    */
  override var errorCount: Int = 0

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???


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
    if (logIsEnabled) log.info(s"start init all workers,id:$syncTaskId")
    initWorkers
    taskManager.start
    log.info(s"~put taskManager:$syncTaskId")
    TaskManager.putTaskManager(syncTaskId, taskManager)
  }

  //正常关闭时会调用，关闭资源
  override def postStop(): Unit

  = {
    log.info(s"syncController processing postStop ,id:$syncTaskId")
    if (!schedulingCommandPool.isShutdown) Try(schedulingCommandPool.shutdownNow())
    TaskManager.removeTaskManager(syncTaskId) // 这步很必要
    //    if (taskManager.isStart) taskManager.close
    taskManager.close
    log.info(s"syncController process postStop success,id:$syncTaskId")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit

  = {
    log.info(s"syncController processing preRestart,id:$syncTaskId")
    //默认的话是会调用postStop，preRestart可以保存当前状态
    controllerChangeStatus(Status.RESTARTING)
    context.become(receive)
    super.preRestart(reason, message)
    log.info(s"syncController processing preRestart complete,id:$syncTaskId")
  }

  override def postRestart(reason: Throwable): Unit

  = {
    log.info(s"syncController processing postRestart,id:$syncTaskId")
    log.info(s"syncController will restart in ${SettingConstant.TASK_RESTART_DELAY} seconds,id:$syncTaskId")
    context.system.scheduler.scheduleOnce(SettingConstant.TASK_RESTART_DELAY seconds, self, SyncControllerMessage(OplogControllerStart))
    //可以恢复之前的状态，默认会调用
    super.postRestart(reason)

  }

  override def supervisorStrategy = {
    AllForOneStrategy() {
      case e: FetcherTimeoutException => {
        log.warning(s"fetcher timeout,try to restart fetcher,id:$syncTaskId")
        Restart
      }
      case e: Exception => {
        controllerChangeStatus(Status.ERROR)
        log.error(s"mongo 2 Hdfs controller crashed,id:$syncTaskId,e:$e")
        e.printStackTrace()
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


