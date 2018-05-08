package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, ActorRef, AllForOneStrategy}
import akka.routing.ConsistentHashingGroup
import akka.routing.ConsistentHashingRouter.ConsistentHashable
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.RowData
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SourceDataBatcher, Status, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder.MysqlBinlogInOrderBatcherManager.IdClassifier
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager

import scala.util.Try

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


  override def receive: Receive = {
    case SyncControllerMessage(msg) => {
      msg match {
        case "start" => {
          context.become(online)
        }
      }
    }
  }

  def online: Receive = {
    case entry: CanalEntry.Entry => {

      import scala.collection.JavaConverters._
      def parseError = {
        log.error(s"parse row data error,id:${syncTaskId}");
        throw new Exception(s"parse row data error,id:${syncTaskId}")
      }

      val rowChange = Try(CanalEntry.RowChange.parseFrom(entry.getStoreValue)).toOption.getOrElse(parseError)
      if (rowChange.getIsDdl) ddlHandler.fold(log.error(s"ddlHandler cannot be found,id:$syncTaskId"))(ref => ref ! IdClassifier(entry, null)) //只会用到entry
      else {
        rowChange.getRowDatasList.asScala.foreach {
          data => router.fold(log.error(s"batcher router cannot be found,id:$syncTaskId"))(ref => ref ! IdClassifier(entry, data))
        }
      }


    }
    case SyncControllerMessage("check") => ddlHandler.fold(log.error(s"ddlHandler cannot be found,id:$syncTaskId"))(ref => ref ! "check")
  }

  def initBatchers = {
    context.actorOf(MysqlBinlogInOrderBatcher.props(mysql2KafkaTaskInfoManager, sinker, -1, true), "ddlHandler")
    lazy val paths = (1 to batcherNum)
      .map(index => context.actorOf(MysqlBinlogInOrderBatcher.props(mysql2KafkaTaskInfoManager, sinker, index), s"batcher$index").path.toString)
    context.actorOf(new ConsistentHashingGroup(paths, virtualNodesFactor = SettingConstant.HASH_MAPPING_VIRTUAL_NODES_FACTOR).props(), "router")
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

object MysqlBinlogInOrderBatcherManager {

  case class IdClassifier(entry: CanalEntry.Entry, rowData: RowData) extends ConsistentHashable {

    import scala.collection.JavaConverters._

    override def consistentHashKey: Any = {
      lazy val prefix = s"${entry.getHeader.getSchemaName}-${entry.getHeader.getTableName}-"
      lazy val key = if (entry.getHeader.getEventType.equals(CanalEntry.EventType.DELETE)) rowData.getBeforeColumnsList.asScala.filter(_.getIsKey).mkString("_") else rowData.getAfterColumnsList.asScala.filter(_.getIsKey).mkString("_")
      prefix + key
    }
  }

}