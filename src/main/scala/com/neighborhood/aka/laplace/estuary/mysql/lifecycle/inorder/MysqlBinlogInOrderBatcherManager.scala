package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, ActorRef, AllForOneStrategy, Props}
import akka.routing.ConsistentHashingGroup
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.{SourceDataBatcher, Status}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.SyncControllerMessage
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{BinlogPositionInfo, DatabaseAndTableNameClassifier, IdClassifier}
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/5/8.
  */
class MysqlBinlogInOrderBatcherManager(
                                        mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,
                                        sinker: ActorRef
                                      ) extends Actor with SourceDataBatcher with ActorLogging {

  val syncTaskId = mysql2KafkaTaskInfoManager.syncTaskId
  //  val mysqlMetaConnection = mysql2KafkaTaskInfoManager.mysqlConnection.fork
  //  var tableMetaCache = buildTableMeta
  val batcherNum = mysql2KafkaTaskInfoManager.batcherNum
  /**
    * 处理ddl语句
    */
  lazy val ddlHandler = context.child("ddlHandler")
  lazy val router = context.child("router")
  var count = 0

  override def receive: Receive = {
    case SyncControllerMessage(msg) => {
      msg match {
        case "start" => {
          context.become(online)
        }
        case x => log.warning(s"batcher offline unhandled message$x,id:$syncTaskId")
      }
    }
  }

  def online: Receive = {
    case entry: CanalEntry.Entry if (entry.getEntryType.equals(CanalEntry.EntryType.TRANSACTIONEND)) => {
      ddlHandler.fold(log.error(s"ddlHandler cannot be found,id:$syncTaskId"))(ref => ref ! BinlogPositionInfo(entry.getHeader.getLogfileName, entry.getHeader.getLogfileOffset))
      count = count +1
    }
    case entry: CanalEntry.Entry => {
      count = count + 1

      if (entry.getHeader.getEventType ==CanalEntry.EventType.ALTER) ddlHandler.fold(log.error(s"ddlHandler cannot be found,id:$syncTaskId"))(ref => ref ! IdClassifier(entry, null)) //只会用到entry
      else {
        router.fold(log.error(s"batcher router cannot be found,id:$syncTaskId"))(ref => ref ! DatabaseAndTableNameClassifier(entry))
        }
      }



    case SyncControllerMessage("check") => ddlHandler.fold(log.error(s"ddlHandler cannot be found,id:$syncTaskId"))(ref => ref ! SyncControllerMessage("check"))
  }

  def initBatchers = {
    context.actorOf(MysqlBinlogInOrderBatcher.props(mysql2KafkaTaskInfoManager, sinker, -1, true), "ddlHandler")
    //编号从1 开始
    //todo 暂时就7ge
    lazy val paths = (1 to 7)
      .map(index => context.actorOf(MysqlBinlogInOrderBatcherPrimaryKeyManager.props(mysql2KafkaTaskInfoManager, sinker, index), s"batcher$index").path.toString)
    context.actorOf(new ConsistentHashingGroup(paths, virtualNodesFactor = SettingConstant.HASH_MAPPING_VIRTUAL_NODES_FACTOR).props().withDispatcher("akka.batcher-dispatcher"), "router")
  }

  @deprecated
  def buildTableMeta(mysqlMetaConnection: MysqlConnection): TableMetaCache = {
    val canalMysqlConnection = mysqlMetaConnection.toCanalMysqlConnection
    canalMysqlConnection.connect()
    val tableMetaCache = new TableMetaCache(mysqlMetaConnection.toCanalMysqlConnection)
    canalMysqlConnection.disconnect()
    tableMetaCache
  }

  /**
    * ********************* 状态变化 *******************
    */
  private def changeFunc(status: Status) = TaskManager.changeFunc(status, mysql2KafkaTaskInfoManager)

  private def onChangeFunc = TaskManager.onChangeStatus(mysql2KafkaTaskInfoManager)

  private def batcherChangeStatus(status: Status) = TaskManager.changeStatus(status, changeFunc, onChangeFunc)

  /**
    * ********************* Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    //状态置为offline
    batcherChangeStatus(Status.OFFLINE)
    initBatchers
    log.info(s"switch batcher to offline,id:$syncTaskId")
  }

  override def postStop(): Unit = {

  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"batcher process preRestart,id:$syncTaskId")
    batcherChangeStatus(Status.RESTARTING)
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"batcher process postRestart,id:$syncTaskId")
    super.postRestart(reason)
  }

  override def supervisorStrategy = {
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
  override var errorCountThreshold: Int = _
  /**
    * 错位次数
    */
  override var errorCount: Int = _

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???
}
object MysqlBinlogInOrderBatcherManager{
  def props(
             mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,
            sinker: ActorRef):Props = Props(new MysqlBinlogInOrderBatcherManager(mysql2KafkaTaskInfoManager,sinker))
}
