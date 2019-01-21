package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.TableSchemaVersionCache.MysqlSchemaVersionCollection
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by john_liu on 2018/6/23.
  */
final class OtherSinkSchemaFlushUtil(
                                      taskManager: MysqlSourceManagerImp with TaskManager
                                    ) {
  /**
    * 同步任务Id
    */
  lazy val syncTaskId = taskManager.syncTaskId
  /**
    * 日志
    */
  lazy val logger = LoggerFactory.getLogger(s"OtherSinkSchemaFlushUtil-$syncTaskId")
  /**
    * 配置
    */
  lazy val config = taskManager.config
//  /**
//    * hive hbase 联动
//    */
//  val hiveMappingHbase = taskManager.hiveMappingHBase
//  /**
//    * hbase操作
//    */
//  lazy val eventualSinkSchemaHandler = taskManager.eventualSinkSchemaHandler

  /**
    * 每次ddl时刷新其他sink
    * 1.hbase建表
    * 2.hive重建表
    *
    * @param mysqlSchemaVersionCollection
    * @return
    */
  def flushOtherSink(mysqlSchemaVersionCollection: => MysqlSchemaVersionCollection): Try[Unit] = {

    lazy val ddlType = mysqlSchemaVersionCollection.ddlType
    lazy val tableId = mysqlSchemaVersionCollection.tableId
    lazy val lastTableId = mysqlSchemaVersionCollection.lastTableId
    lazy val dbId = mysqlSchemaVersionCollection.dbId
    lazy val version = mysqlSchemaVersionCollection.version
    lazy val flushHBase: Try[Unit] = ddlType match {
      case DdlType.DROP => dropHBaseTable(dbId, tableId)
      case DdlType.TRUNCATE => for {
        _ <- dropHBaseTable(dbId, lastTableId) //删除上个tableId
        _ <- createHbaseTable(dbId, tableId) //创建当前tableId
      } yield ()
      case _ => createHbaseTable(dbId, tableId)
    }
    lazy val flushHive: Try[Unit] = ddlType match {
      case DdlType.CREATE => createHiveTable(tableId, version)
      case DdlType.TRUNCATE => for {
        _ <- dropHiveTable(lastTableId, version - 1)
        _ <- createHiveTable(tableId, version)
      } yield ()
      case DdlType.AlTER => for {
        _ <- dropHiveTable(tableId, version - 1)
        _ <- createHiveTable(tableId, version)
      } yield ()
      case DdlType.DROP => dropHiveTable(tableId, version)
      case _ => Try(logger.warn(s"$tableId cannot recognized!!,id:$syncTaskId"))
    }


    for {
      _ <- flushHBase
      _ <- Try(logger.info(s"flushHBase finish,id:$syncTaskId"))
      _ <- flushHive
      _ <- Try(logger.info(s"flushHive finish,id:$syncTaskId"))
      _ <- Try(logger.info(s"flush other sink finished,ddlType:${ddlType},tableId:$tableId,id:$syncTaskId"))
    } yield ()

  }

  /**
    * 创建hbaseTable
    *
    * @return
    */
  private def createHbaseTable(dbId: String, tableId: String): Try[Unit] = Try {
    logger.info(s"start createHbaseTable:$dbId.$tableId,id:$syncTaskId")
//    lazy val tableInfo = HBaseTableInfo(dbId, tableId)
//    taskManager.eventualSinkSchemaHandler.createTableIfNotExists(tableInfo)

  }

  /**
    * 创建hiveTable
    *
    * @return
    */
  private def createHiveTable(tableId: String, version: Int): Try[Unit] = Try {
    logger.info(s"start createHiveTable,$tableId version:$version,id:$syncTaskId")
//    hiveMappingHbase.createUniqueTable(
//      tableId.toLowerCase,
//      version
//    ) match {
//      case false => logger.error(s"cannot create hive table:$tableId,id:$syncTaskId")
//      case true => logger.info(s"create hive table:$tableId,id:$syncTaskId")
//    }
  }

  /**
    * 删除HBaseTable
    *
    * @return
    */
  private def dropHBaseTable(dbId: String, tableId: String): Try[Unit] = Try {
    logger.info(s"start drop HBaseTable,:$dbId.$tableId,id:$syncTaskId")
//    lazy val tableInfo = HBaseTableInfo(dbId, tableId)
//    eventualSinkSchemaHandler.dropTableIfExists(tableInfo)
  }

  /**
    * 删除HiveTable
    *
    * @return
    */
  private def dropHiveTable(tableId: String, version: Int): Try[Unit] = Try {
    logger.info(s"start drop hiveTable $tableId version:$version,id:$syncTaskId")
//    hiveMappingHbase.deleteUniqueTable(tableId, version)
  }
}