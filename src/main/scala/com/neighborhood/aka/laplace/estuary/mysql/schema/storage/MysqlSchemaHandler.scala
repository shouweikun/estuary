package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import java.util.concurrent.ConcurrentHashMap

import com.neighborhood.aka.laplace.estuary.bean.exception.other.DeplicateInitializationException
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.SchemaEntry.{EmptySchemaEntry, MysqlConSchemaEntry}
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.typesafe.config.Config

import scala.util.Try

/**
  * Created by john_liu on 2018/5/31.
  * todo 线程安全
  *
  * 数据库表设计 @link http://wiki.puhuitech.cn/pages/viewpage.action?pageId=30059341
  */

class MysqlSchemaHandler(
                          /**
                            * 数据库连接，使用mysqlConnection
                            */
                          val mysqlConnection: MysqlConnection,
                          val syncTaskId: String,
                          val config: Config,
                          dbName: String = "",
                          tableName: String = ""
                        ) {


  /**
    * 元数据库的库名
    */
  val schemaDbName = ""
  /**
    * 表字段信息 表名
    */
  val fieldInfoTableName = "trickle3_schema_field_info"
  /**
    * 数据库信息 表名
    */
  val databaseSchemaTableName = "trickle3_schema_dbs"
  /**
    * 表信息 表名
    */
  val tableSchemaTableName = "trickle3_schema_tables"
  /**
    * 表版本 表名
    */
  val schemaVersionTableName = "trickle3_schema_version"
  /**
    * HBase-Hive映射 表名
    */
  val schemaMappingTableName = "trickle3_schema_hbase_hive_mapping"


  val tableVersionMap: ConcurrentHashMap[String, TableSchemaVersionCache] = new ConcurrentHashMap[String, TableSchemaVersionCache]()
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
    * information_schema.TABLES a
    * LEFT JOIN information_schema. COLUMNS b ON a.table_name = b.TABLE_NAME
    */
  val sourceInfoSqlPrefix = "SELECT\na.table_name tableName,\na.table_comment tableComment,\nunix_timestamp(a.create_time) tableCreateTime,       \nunix_timestamp(a.update_time) tableUpdateTime,\nb.ordinal_position fieldIndex,\nb.COLUMN_NAME fieldName,\nb.column_comment fieldComment,\nb.column_type  fieldType,\nb.column_key fieldConstraint\nFROM\ninformation_schema. TABLES a\nLEFT JOIN information_schema. COLUMNS b ON a.table_name = b.TABLE_NAME"

  /**
    * SELECT
    * db_name,
    * trickle3_schema_dbs_id,
    * FROM
    * $schemaDbName.$databaseSchemaTableName
    * WHERE
    * db_name  = '$dbName'
    *
    *
    * * @param dbName
    *
    * @return
    */
  def isInitialized(dbName: String): Boolean = {
    lazy val sql = s"SELECT\ndb_name,\ntrickle3_schema_dbs_id,\nFROM\n     $schemaDbName.$databaseSchemaTableName\nWHERE\ndb_name  = '$dbName'"
    val conn = mysqlConnection.fork
    conn.connect()
    val size = conn.queryForScalaList(sql).size
    conn.disconnect()
    size match {
      case 0 => false
      case 1 => true
      case x => throw new DeplicateInitializationException(s"find $x records when check Initialization on dbName:$dbName,id:$syncTaskId")
    }
  }

  def getTableVersion(dbName: String, tableName: String): Int = ???

  def upsertSchema(sql: => String): Try[Unit] = Try {
    val conn = mysqlConnection.fork
    conn.connect()
    conn.update(sql);
    conn.disconnect()
  }

  def upsertSchema(schemaEntry: MysqlConSchemaEntry) = {

  }

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
        lazy val sql = s"$sourceInfoSqlPrefix\nWHERE\na.table_schema = '$dbName' and a.table_name = '$tableName'"
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
        lazy val sql = s"$sourceInfoSqlPrefix\nWHERE\na.table_schema = '$dbName' \nORDER BY\na.table_name"
        connection
          .queryForScalaList(sql)
          .groupBy(_ ("tableName"))
          .map(kv => (kv._1.toString -> kv._2))
    }
    re
  }

  /**
    *
    * @param dbName
    */
  def createCache(dbName: String): Unit = {
    //todo
  }

  def handleInitializationTask(dbName: String):Unit = {
    //todo
  }

  private def convertRow2ConSchemaEntry(
                                         row: Map[String, Any],
                                         isOriginal: Boolean = false,
                                         binlogJournalName: String = "mysql-bin.000000",
                                         binlogPosition: Long = 4l
                                       ): MysqlConSchemaEntry = {
    //todo
    lazy val effectTimestamp = System.currentTimeMillis()
    lazy val dbName = row("dbName").toString
    lazy val dbId = if (isOriginal) s"mysql@${dbName}@$effectTimestamp" else ""
    lazy val tableName = row("tableName").toString
    lazy val tableId = if (isOriginal) s"mysql@${dbName}-@{$tableName}$effectTimestamp" else "" //TODO
    lazy val tableComment = row("tableComment").toString
    lazy val fieldName = row("fieldName").toString
    lazy val fieldIndex = row("fieldIndex").toString.toInt
    lazy val fieldComment = row("fieldComment").toString
    lazy val fieldType = row("fieldType").toString
    lazy val fieldConstraint = row("fieldConstraint").toString
    lazy val timestamp = row.get("tableUpdateTime").getOrElse(row("tableCreateTime")).toString.toLong * 1000
    lazy val version = if (isOriginal) 0l else tableVersionMap.get(dbName).getLatestVersion(tableName) + 1

    MysqlConSchemaEntry(
      dbId,
      dbName,
      tableId,
      tableName,
      tableComment,
      fieldIndex,
      fieldName,
      fieldComment,
      fieldType,
      fieldConstraint,
      version,
      timestamp,
      binlogJournalName,
      binlogPosition,
      isOriginal
    )

  }
}
