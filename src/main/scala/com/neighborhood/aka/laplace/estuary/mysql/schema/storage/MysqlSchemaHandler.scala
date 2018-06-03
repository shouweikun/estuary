package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import java.util.concurrent.ConcurrentHashMap

import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.SchemaEntry.{EmptySchemaEntry, MysqlConSchemaEntry}
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.typesafe.config.Config

import scala.util.Try

/**
  * Created by john_liu on 2018/5/31.
  * todo 线程安全
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
  val tableVersionMap: ConcurrentHashMap[String, Int] = new ConcurrentHashMap[String, Int]()
  /**
    * SELECT
    *a.table_schema dbName,
    *a.table_name tableName,             //表名
    *a.table_comment tableComment,       //表注释
    *a.create_time tableCreateTime       //表创建时间
    *a.update_time tableUpdateTime       //表更新时间
    *b.ordinal_position fieldIndex       //字段序号
    *b.COLUMN_NAME fieldName,            //字段名称
    *b.column_comment fieldComment,      //字段注释
    *b.column_type  fieldType,           //字段类型
    *b.column_key fieldConstraint        //字段约束
    * FROM
    * information_schema. TABLES a
    * LEFT JOIN information_schema. COLUMNS b ON a.table_name = b.TABLE_NAME
    */
  val infoSqlPrefix = "SELECT\na.table_name tableName,\na.table_comment tableComment,\nunix_timestamp(a.create_time) tableCreateTime,       \nunix_timestamp(a.update_time) tableUpdateTime,\nb.ordinal_position fieldIndex,\nb.COLUMN_NAME fieldName,\nb.column_comment fieldComment,\nb.column_type  fieldType,\nb.column_key fieldConstraint\nFROM\ninformation_schema. TABLES a\nLEFT JOIN information_schema. COLUMNS b ON a.table_name = b.TABLE_NAME"

  def isInitialized(dbName: String): Boolean = {
    //todo
    ???
  }

  def upsertSchema(sql: => String): Try[Unit] = Try(mysqlConnection.reconnect())
    .map {
      _ =>
        mysqlConnection.update(sql);
        mysqlConnection.disconnect()
    }

  def upsertSchema(schemaEntry: MysqlConSchemaEntry) = upsertSchema(schemaEntry.convertEntry2Sql(targetDbName, targetTableName))

  def upsertSchema(rowMap: Map[String, Any], isOriginal: Boolean) = upsertSchema(convertRow2ConSchemaEntry(rowMap, isOriginal))

  def getSchemas(sql: => String): Try[List[MysqlConSchemaEntry]] = Try(mysqlConnection.reconnect()).map {
    _ =>
      val result = mysqlConnection.queryForScalaList(sql)
      mysqlConnection.disconnect()
      if (result.size == 0) List.empty else result.map(convertRow2ConSchemaEntry(_))

  }

  def getAllSchemas(dbName: String, tableName: String): Try[List[SchemaEntry]] = {
    lazy val sql = s"SELECT * FROM $targetDbName.$targetTableName where db_name = '$dbName' and table_name = '$tableName'"
    getSchemas(sql)
  }

  def getLatestSchema(dbName: String, tableName: String): Try[SchemaEntry] = {
    lazy val sql = s"SELECT * FROM $targetDbName.$targetTableName where db_name = '$dbName' and table_name = '$tableName' ORDER BY version DESC limit 1"
    getSchemas(sql)
      .map {
        list =>
          list match {
            case Nil => new EmptySchemaEntry
            case _ => list(0)
          }
      }
  }

  //todo 有问题
  def getCorrespondingSchema(dbName: String, tableName: String, timestamp: Long, binlogJournalName: String, binlogOffset: Long): Try[SchemaEntry] = {

    def convertBinlogPosition2Long(binlogJournalName: String, binlogOffset: Long) = s"${binlogJournalName.split('.')(1)}$binlogOffset".toLong

    lazy val sql = s"SELECT * FROM $targetDbName.$targetTableName where db_name = '$dbName' and table_name = '$tableName' WHERE timestamp <= $timestamp ORDER BY version "
    getSchemas(sql)
      .map {
        list =>
          list match {
            case Nil => new EmptySchemaEntry
            case _ => {
              list
                .filter(x => convertBinlogPosition2Long(x.binlogFileName, x.binlogPosition) < convertBinlogPosition2Long(binlogJournalName, binlogOffset))
                .maxBy(x => convertBinlogPosition2Long(x.binlogFileName, x.binlogPosition))
            }
          }

      }
  }

  def getSingleTableInfo(dbName: String, tableName: String): Try[List[Map[String, Any]]] = {
    val connection = mysqlConnection.fork
    Try {
      connection.connect()
    }.map {
      _ =>
        lazy val sql = s"$infoSqlPrefix\nWHERE\na.table_schema = '$dbName' and a.table_name = '$tableName'"
        connection.queryForScalaList(sql)
    }
  }

  /**
    * SELECT
    *a.table_schema dbName,
    *a.table_name tableName,             //表名
    *a.table_comment tableComment,       //表注释
    *a.create_time tableCreateTime       //表创建时间
    *a.update_time tableUpdateTime       //表更新时间
    *b.ordinal_position fieldIndex       //字段序号
    *b.COLUMN_NAME fieldName,            //字段名称
    *b.column_comment fieldComment,      //字段注释
    *b.column_type  fieldType,           //字段类型
    *b.column_key fieldConstraint        //字段约束
    * FROM
    * information_schema. TABLES a
    * LEFT JOIN information_schema. COLUMNS b ON a.table_name = b.TABLE_NAME
    * WHERE
    *a.table_schema = '$dbName'
    * ORDER BY
    *a.table_name
    *
    * @param dbName
    * @return
    */
  def getAllTablesInfo(dbName: String): Try[Map[String, List[Map[String, Any]]]] = {
    val connection = mysqlConnection.fork
    lazy val re = Try {
      connection.connect()
    }.map {
      _ =>
        lazy val sql = s"$infoSqlPrefix\nWHERE\na.table_schema = '$dbName' \nORDER BY\na.table_name"
        connection
          .queryForScalaList(sql)
          .groupBy(_ ("tableName"))
          .map(kv => (kv._1.toString -> kv._2))
    }
    re
  }

  private def convertRow2ConSchemaEntry(row: Map[String, Any], isOriginal: Boolean = false): MysqlConSchemaEntry = {
    //todo
    lazy val dbName = row("dbName").toString
    lazy val tableName = row("tableName").toString
    lazy val tableComment = row("tableComment").toString
    lazy val fieldName = row("fieldName").toString
    lazy val fieldIndex = row("fieldIndex").toString.toInt
    lazy val fieldComment = row("fieldComment").toString
    lazy val fieldType = row("fieldType").toString
    lazy val fieldConstraint = row("fieldConstraint").toString
    lazy val timestamp = row.get("tableUpdateTime").getOrElse(row("tableCreateTime")).toString.toLong * 1000
    lazy val version = if (isOriginal) 0 else
      ???

  }
}