package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.control.hbase

import java.util.concurrent.{ExecutorService, Executors}

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{ActorRef, OneForOneStrategy, Props}
import com.neighborhood.aka.laplace.estuary.core.akkaUtil.SyncDaemonCommand._
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SyncControllerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{ListenerMessage, SinkerMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.sink.hbase.HBaseSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mongo.SettingConstant
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.control.OplogControllerCommand._
import com.neighborhood.aka.laplace.estuary.mongo.sink.hbase.HBaseBeanImp
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoSourceBeanImp}
import com.neighborhood.aka.laplace.estuary.mongo.task.hbase.{Mongo2HBaseAllTaskInfoBean, Mongo2HBaseTaskInfoBeanImp, Mongo2HBaseTaskInfoManager}
import org.I0Itec.zkclient.exception.ZkTimeoutException

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.Try

/**
  * Created by john_liu on 2019/3/6.
  *
  * @note 子任务命名规则:s"$syncTaskId::$tableName"
  * @author neighborhood.aka.lapalce
  */
final class Oplog2HBaseMultiInstanceController(
                                                val allTaskInfoBean: Mongo2HBaseAllTaskInfoBean
                                              ) extends SyncControllerPrototype[MongoConnection, HBaseSinkFunc] {

  protected val schedulingCommandPool: ExecutorService = Executors.newFixedThreadPool(3)
  /**
    * 必须要用这个，保证重启后，之前的定时发送任务都没了
    */
  implicit protected val scheduleTaskPool: ExecutionContextExecutor = ExecutionContext.fromExecutor(schedulingCommandPool)
  override val taskBean: Mongo2HBaseTaskInfoBeanImp = allTaskInfoBean.taskRunningInfoBean
  override val sourceBean: MongoSourceBeanImp = allTaskInfoBean.sourceBean
  override val sinkBean: HBaseBeanImp = allTaskInfoBean.sinkBean

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskBean.syncTaskId

  /**
    * 任务信息管理器
    */
  override val taskManager: Mongo2HBaseTaskInfoManager = buildManager


  val isCosting = taskManager.isCosting
  val isCounting = taskManager.isCounting
  val isPowerAdapted = taskManager.isPowerAdapted

  log.info(s"Oplog2HBaseMutliInstanceController start build,id:$syncTaskId")

  override def resourceManager: Mongo2HBaseTaskInfoManager = taskManager

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
    case OplogControllerStopAndRestart => restartBySupervisor
    case ListenerMessage(msg) => log.warning(s"syncController online unhandled message:$msg,id:$syncTaskId")
    case SinkerMessage(msg) => log.warning(s"syncController online unhandled message:${SinkerMessage(msg)},id:$syncTaskId")
    case SyncControllerMessage(OplogControllerCheckRunningInfo) => checkInfo
    case SyncControllerMessage(OplogControllerCollectChildInfo) => collectChildInfo
    case m@ExternalSuspendCommand(`syncTaskId`) => context.children.foreach(ref => ref ! m)
    case m@ExternalResumeCommand(`syncTaskId`) => context.children.foreach(ref =>ref ! m)
    case m@ExternalSuspendTimedCommand(`syncTaskId`,_) => context.children.foreach(ref =>ref ! m)
    case msg => log.warning(s"syncController online unhandled message:${msg},id:$syncTaskId")
  }

  /**
    * 任务资源管理器的构造的工厂方法
    *
    * @return 构造好的资源管理器
    *
    */
  override def buildManager: Mongo2HBaseTaskInfoManager = {
    log.info(s"start to build Mongo2HBaseTaskInfoManager,id:$syncTaskId")
    new Mongo2HBaseTaskInfoManager(allTaskInfoBean, context.system.settings.config)
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
    assert(sourceBean.concernedNs.nonEmpty)
    sourceBean.concernedNs.foreach {
      tableName =>
        val spSourceBean = sourceBean.copy()(concernedNs = Array(tableName), ignoredNs = sourceBean.ignoredNs)
        val newTaskName = buildChildTaskName(tableName)
        val spTaskBean = taskBean.copy(syncTaskId = newTaskName)(
          logEnabled = false,
          mongoOffset = taskBean.mongoOffset,
          isCosting = taskBean.isCosting,
          isCounting = taskBean.isCounting,
          isProfiling = taskBean.isProfiling,
          isPowerAdapted = taskBean.isPowerAdapted,
          partitionStrategy = taskBean.partitionStrategy,
          syncStartTime = taskBean.syncStartTime,
          batchThreshold = taskBean.batchThreshold,
          batcherNum = taskBean.batcherNum,
          sinkerNum = taskBean.sinkerNum)
        val spTotalInfoBean = allTaskInfoBean.copy(sourceBean = spSourceBean, taskRunningInfoBean = spTaskBean)
        val props = Oplog2HBaseController.props(spTotalInfoBean)
        context.actorOf(props, newTaskName)
    }
  }


  /**
    *
    */
  protected def startAllWorkers: Unit = {
    scheduleCheckInfoCommand(self)
    sendCollectChildInfoCommand(self)
    context.children.foreach {
      ref => ref ! ExternalStartCommand
    }
  }


  def collectChildInfo: Unit = {
    val tem = 3
    val childTaskManagers = sourceBean.concernedNs.map(x => TaskManager.getTaskManager(buildChildTaskName(x))).withFilter(_.isDefined).map(_.get)
    childTaskManagers.map(_.syncTaskId).toList.diff(sourceBean.concernedNs.toList.map(buildChildTaskName(_))) match {
      case Nil => //do nothing
      case list => log.error(s"table:${list.mkString(",")} can not found when collect Child info,id:$syncTaskId")
    }
    val lastFetchCount = taskManager.fetchCount.get()
    val fetchCount = childTaskManagers.map(_.fetchCount.get()).sum
    val lastBatchCount = taskManager.batchCount.get()
    val batchCount = childTaskManagers.map(_.batchCount.get()).sum
    val lastSinkCount = taskManager.sinkCount.get()
    val sinkCount = childTaskManagers.map(_.sinkCount.get()).sum
    taskManager.fetchCount.set(fetchCount)
    taskManager.batchCount.set(batchCount)
    taskManager.sinkCount.set(sinkCount)
    taskManager.fetchCost.set(childTaskManagers.map(_.fetchCost.get()).sum / childTaskManagers.size)
    taskManager.batchCost.set(childTaskManagers.map(_.batchCost.get()).sum / childTaskManagers.size)
    taskManager.sinkCost.set(childTaskManagers.map(_.sinkCost.get()).sum / childTaskManagers.size)
    taskManager.sinkCountPerSecond.set((sinkCount - lastSinkCount) / tem)
    taskManager.batchCountPerSecond.set((batchCount - lastBatchCount) / tem)
    taskManager.fetchCountPerSecond.set((fetchCount - lastFetchCount) / tem)
    taskManager.fetchDelay.set(-1)
    taskManager.sinkerLogPosition.set(childTaskManagers.map(_.sinkerLogPosition.get()).mkString("\n"))
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

  private def sendCollectChildInfoCommand(ref: ActorRef): Unit = {
    log.info(s" schedule `OplogControllerCollectChildInfo` message per ${SettingConstant.CHECK_COLLECT_OFFSET_INTERVAL},id:$syncTaskId")
    context.system.scheduler.schedule(15 seconds, 3 seconds, ref, SyncControllerMessage(OplogControllerCollectChildInfo)
    )
  }

  private def buildChildTaskName(tableName: String): String = s"$syncTaskId::$tableName"

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
    log.info(s"mongo2hbase mutli instance syncController processing postRestart,id:$syncTaskId")
    log.info(s"syncController will restart in ${SettingConstant.TASK_RESTART_DELAY} seconds,id:$syncTaskId")

    context.system.scheduler.scheduleOnce(SettingConstant.TASK_RESTART_DELAY seconds, self, SyncControllerMessage(OplogControllerStart))
    //可以恢复之前的状态，默认会调用
    super.postRestart(reason)

  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: ZkTimeoutException => {
        controllerChangeStatus(Status.ERROR)
        Restart
      }
      case e: Exception => {
        controllerChangeStatus(Status.ERROR)
        Restart

      }
      case error: Error => {
        controllerChangeStatus(Status.ERROR)
        Restart
      }
      case _ => {
        controllerChangeStatus(Status.ERROR)
        Restart
      }
    }
  }

}

object Oplog2HBaseMultiInstanceController {
  def props(allTaskInfoBean: Mongo2HBaseAllTaskInfoBean): Props = Props(new Oplog2HBaseMultiInstanceController(allTaskInfoBean))
}
