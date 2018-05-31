package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.typesafe.config.Config

import scala.util.Try

/**
  * Created by john_liu on 2018/5/31.
  */
class MysqlSchemaHandler(
                          /**
                            * 数据库连接，使用mysqlConnection
                            */
                          val mysqlConnection: MysqlConnection,
                          val config: Config,
                          dbName: String = "",
                          tableName: String = ""
                        ) {


  val targetDbName = if (dbName != null && !dbName.isEmpty) dbName else config.getString("schema.target.db")
  val targetTableName = if (tableName != null && !tableName.isEmpty) tableName else config.getString("schema.target.table")

  def upsertSchema(sql: => String): Try[Unit] = Try(mysqlConnection.reconnect())
    .map {
      _ =>
        mysqlConnection.update(sql);
        mysqlConnection.disconnect()
    }

  def upsertSchema(schemaEntry: SchemaEntry) = upsertSchema(schemaEntry.convertEntry2Sql(targetDbName, targetTableName))

  def getSchemas(sql: => String): Try[List[SchemaEntry]] = Try(mysqlConnection.reconnect()).map {
    _ =>
      val result = mysqlConnection.queryForScalaList(sql)
      mysqlConnection.disconnect()
      result.map(convertRow2SchemaEntry(_))
  }

  def getAllSchemas(dbName: String, tableName: String): Try[List[SchemaEntry]] = {
    lazy val sql = s"SELECT * FROM $targetDbName.$targetTableName where db_name = '$dbName' and table_name = '$tableName'"
    getSchemas(sql)
  }

  def getLatestSchema(dbName: String, tableName: String): Try[SchemaEntry] = {
    lazy val sql = s"SELECT * FROM $targetDbName.$targetTableName where db_name = '$dbName' and table_name = '$tableName' ORDER BY version DESC limit 1"
    getSchemas(sql)
      .map(x => x(0))
  }

  //todo 有问题
  def getCorrespondingSchema(dbName: String, tableName: String, timestamp: Long): Try[SchemaEntry] = {
    lazy val sql = s"SELECT * FROM $targetDbName.$targetTableName where db_name = '$dbName' and table_name = '$tableName' WHRER timestamp <= $timestamp ORDER BY version DESC limit 1"
    getSchemas(sql)
      .map(x => x(0))
  }


  def getSchemas(dbName: String, tableName: String)

  def convertRow2SchemaEntry(row: Map[String, Any]): SchemaEntry = {
    //todo
    ???

  }
}
