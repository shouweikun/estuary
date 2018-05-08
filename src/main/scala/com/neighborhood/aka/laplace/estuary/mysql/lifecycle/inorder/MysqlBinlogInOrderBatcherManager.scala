package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.routing.ConsistentHashingRouter.{ConsistentHashMapping, ConsistentHashable}
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SourceDataBatcher, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/5/8.
  */
class MysqlBinlogInOrderBatcherManager(
                                        mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,
                                        sinker: ActorRef
                                      ) extends Actor with SourceDataBatcher with ActorLogging {

  val syncTaskId = mysql2KafkaTaskInfoManager.syncTaskId
  val mysqlMetaConnection = mysql2KafkaTaskInfoManager.mysqlConnection.fork
  var tableMetaCache = buildTableMeta
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
      entry.getHeader.getEventType match {
        case CanalEntry.EventType.ALTER => {
          //重新获得tablemataCache
          tableMetaCache = buildTableMeta
          ddlHandler.fold {
            log.error(s"ddlHandler cannot be null ,id:${syncTaskId}");
            throw new Exception("ddlHandler cannot be null ,id:${syncTaskId}")
          } {
            ref => ref ! entry
          }
        }
        //都是dml
        case _ => {
          import scala.collection.JavaConverters._
          val tableName = entry.getHeader.getTableName
          val dbName = entry.getHeader.getSchemaName
          val primaryKey = tableMetaCache
            .getTableMeta(dbName, tableName)
            .getPrimaryFields
            .asScala
            .map(_.getColumnName)
          entry.getAllFields.keySet().asScala.filter(x=>x.ge)

        }
      }
    }
  }

  def buildTableMeta: TableMetaCache = {
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

  sealed case class IdClassifier(entry: CanalEntry, key: String) extends ConsistentHashable {
    override def consistentHashKey: Any = key
  }

}