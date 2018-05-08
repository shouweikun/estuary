package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.routing.ConsistentHashingRouter.{ConsistentHashMapping, ConsistentHashable}
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{RowChange, RowData}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SourceDataBatcher, SyncControllerMessage}
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
      if (rowChange.getIsDdl) ddlHandler.fold(log.error(s"ddlHandler cannot be found,id:$syncTaskId"))(ref => ref ! entry)
      else {
        rowChange.getRowDatasList.asScala.foreach {
          data => router.fold(log.error(s"batcher router cannot be found,id:$syncTaskId"))(ref => ref ! IdClassifier(entry, data))
        }
      }


    }
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

  sealed case class IdClassifier(entry: CanalEntry.Entry, rowData: RowData) extends ConsistentHashable {

    import scala.collection.JavaConverters._

    override def consistentHashKey: Any = {

      val key = if (entry.getHeader.getEventType.equals(CanalEntry.EventType.DELETE)) rowData.getBeforeColumnsList.asScala.filter(_.getIsKey).mkString("_") else rowData.getAfterColumnsList.asScala.filter(_.getIsKey).mkString("_")
      key
    }
  }

}