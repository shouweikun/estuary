package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{ActorRef, AllForOneStrategy, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.bean.exception.batch.UnsupportedEntryException
import com.neighborhood.aka.laplace.estuary.bean.exception.other.WorkerInitialFailureException
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataBatcherManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, FetcherMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderBatcherCommand._
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderBatcherEvent.MysqlBinlogInOrderBatcherStarted
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp.MysqlBinlogInOrderBatcherMysqlManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{BinlogPositionInfo, DatabaseAndTableNameClassifier}
import com.neighborhood.aka.laplace.estuary.mysql.source.{MysqlConnection, MysqlSourceManagerImp}
import com.neighborhood.aka.laplace.estuary.mysql.utils.{CanalEntryJsonHelper, CanalEntryTransUtil}

/**
  * Created by john_liu on 2018/5/8.
  *
  * @author neighborhood.aka.laplace
  * @tparam B Sink
  */
abstract class MysqlBinlogInOrderBatcherManager[B <: SinkFunc](
                                                                /**
                                                                  * 任务信息管理器
                                                                  */
                                                                override val taskManager: MysqlSourceManagerImp with TaskManager,

                                                                /**
                                                                  * sinker 的ActorRef
                                                                  */
                                                                override val sinker: ActorRef

                                                              ) extends SourceDataBatcherManagerPrototype[MysqlConnection, B] {
  /**
    * 编号
    */
  override val num: Int = -1
  /**
    * 事件收集器
    */
  override lazy val eventCollector: Option[ActorRef] = taskManager.eventCollector
  /**
    * 是否是顶层manager
    */
  override val isHead: Boolean = true

  /**
    * 同步任务id
    */
  override val syncTaskId = taskManager.syncTaskId

  /**
    * 是否同步
    */
  val isSync = taskManager.isSync

  /**
    * 分区策略
    */
  val partitionStrategy = taskManager.partitionStrategy

  /**
    * batcher的数量，对于当前例子是PrimaryKeyBatcher的数量
    */
  val batcherNum = taskManager.batcherNum
  /**
    * batcherNameToLoad
    */
  val batcherNameToLoad:Map[String,String] =taskManager.batcherNameToLoad
  /**
    * router的ActorRef
    */
  lazy val router = context.child(routerName)

  /**
    * 特别信息处理
    */
  lazy val specialInfoSender = context.child(specialInfoSenderName)


  override def receive: Receive = {
    case SyncControllerMessage(MysqlBinlogInOrderBatcherStart) => switch2Online
    case SyncControllerMessage(msg) => log.warning(s"batcher offline unhandled message$msg,id:$syncTaskId")
  }

  protected def online: Receive = {

    case entry: CanalEntry.Entry => DispatchEntry(entry)
    case FetcherMessage(enty:CanalEntry.Entry) =>DispatchEntry(enty)
    case SyncControllerMessage(MysqlBinlogInOrderBatcherCheckHeartbeats) => dispatchCheckMessage
  }

  /**
    * 换成online
    */
  protected def switch2Online: Unit = {
    context.become(online, true)
    batcherChangeStatus(Status.ONLINE)
    eventCollector.map(ref => ref ! MysqlBinlogInOrderBatcherStarted)
  }

  /**
    * 初始化Batchers
    * 包含SpecialSender 和 普通batchers和router
    */
  override protected def initBatchers: Unit

  /**
    * 分发entry
    *
    * @param entry
    */
  private def DispatchEntry(entry: => CanalEntry.Entry): Unit = {
    lazy val isTransactional = CanalEntryTransUtil.isTransactionEnd(entry)
    lazy val isDml = CanalEntryTransUtil.isDml(entry)
    lazy val isDdl = CanalEntryTransUtil.isDdl(entry)

    if (isTransactional) dispatchTransactionEndEntry(entry);
    else if (isDml) dispatchDmlEntry(entry)
    else if (isDdl) dispatchDdlEntry(entry)
    else throw new UnsupportedEntryException(s"${
      CanalEntryJsonHelper.headerToJson(entry.getHeader)
    } is not supported,id:$syncTaskId")
  }

  /**
    * 由ddl处理器处理Transactional事件
    * 直接发送给ddlHandler
    */
  protected def dispatchTransactionEndEntry(entry: => CanalEntry.Entry): Unit = {
    specialInfoSender
      .fold(log.error(s"$specialInfoSenderName cannot be found,id:$syncTaskId"))(ref => ref ! BatcherMessage(BinlogPositionInfo(entry.getHeader.getLogfileName, entry.getHeader.getLogfileOffset, entry.getHeader.getExecuteTime)))
  }

  /**
    * 先进行库表级的分离
    */
  private def dispatchDmlEntry(entry: => CanalEntry.Entry): Unit = {
    router.fold(log.error(s"batcher router cannot be found,id:$syncTaskId"))(ref => ref ! DatabaseAndTableNameClassifier(entry))
  }

  /**
    * ddl语句的库表级分离
    *
    * @param entry
    */
  private def dispatchDdlEntry(entry: CanalEntry.Entry): Unit = {
    log.info(s"dispatch ddl entry:${
      CanalEntryJsonHelper.headerToJson(entry.getHeader)
    },id:$syncTaskId")
    router.fold(log.error(s"batcher router cannot be found,id:$syncTaskId"))(ref => ref ! DatabaseAndTableNameClassifier(entry))
  }

  /**
    * 发送控制信号
    */
  protected def dispatchCheckMessage: Unit = specialInfoSender.fold(log.error(s"ddlHandler cannot be found,id:$syncTaskId"))(ref => ref ! BatcherMessage(MysqlBinlogInOrderBatcherCheckHeartbeats))


  /**
    * ********************* 状态变化 *******************
    */
  protected def changeFunc(status: Status)

  = TaskManager.changeFunc(status, taskManager)

  protected def onChangeFunc

  = TaskManager.onChangeStatus(taskManager)

  protected def batcherChangeStatus(status: Status)

  = TaskManager.changeStatus(status, changeFunc, onChangeFunc)

  /**
    * ********************* Actor生命周期 *******************
    */
  override def preStart(): Unit

  = {
    //状态置为offline
    batcherChangeStatus(Status.OFFLINE)
    initBatchers
    log.info(s"switch batcher to offline,id:$syncTaskId")
  }

  override def postStop(): Unit

  = {
    log.info(s"batcher process postStop,id:$syncTaskId")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit

  = {
    log.info(s"batcher process preRestart,id:$syncTaskId")
    batcherChangeStatus(Status.RESTARTING)
    super.preRestart(reason, message)

  }

  override def postRestart(reason: Throwable): Unit

  = {
    log.info(s"batcher process postRestart,id:$syncTaskId")
    super.postRestart(reason)
  }

  override def supervisorStrategy

  = {
    AllForOneStrategy() {
      case _ => {
        batcherChangeStatus(Status.ERROR)
        Escalate
      }
    }
  }

  /**
    * 错位次数阈值
    */
  override val errorCountThreshold: Int = 1

  /**
    * 错位次数
    */
  override var errorCount: Int = 0

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit

  = ???
}

object MysqlBinlogInOrderBatcherManager {
  /**
    * MysqlBinlogInOrderBatcherManager的工厂方法
    *
    * @param name        名称
    * @param taskManager taskManager
    * @param sinker      sinker的ActorRef
    * @return 构建好的Props
    */
  def buildMysqlBinlogInOrderBatcherManager(name: String, taskManager: MysqlSourceManagerImp with TaskManager, sinker: ActorRef): Props = {
    name match {
      case MysqlBinlogInOrderBatcherMysqlManager.name => MysqlBinlogInOrderBatcherMysqlManager.props(taskManager, sinker)
      case _ => throw new WorkerInitialFailureException(s"cannot build MysqlBinlogInOrderBatcherManager name item match $name")
    }
  }
}
