package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import java.util.concurrent.ConcurrentHashMap

import com.neighborhood.aka.laplace.estuary.bean.exception.schema.{DeplicateInitializationException, SchemaIsNotInitializedException}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.SchemaEntry.{EmptySchemaEntry, MysqlConSchemaEntry}
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.TableSchemaVersionCache.MysqlSchemaVersionCollection
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

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
                          val config: Config
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

  lazy val log = LoggerFactory.getLogger(s"MysqlSchemaHandler-$syncTaskId")
  lazy val dbNameMappingdbIdMap: ConcurrentHashMap[String, String] = new ConcurrentHashMap[String, String]()

  lazy val tableVersionCacheMap: ConcurrentHashMap[String, TableSchemaVersionCache] = new ConcurrentHashMap[String, TableSchemaVersionCache]()

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


  /**
    * 根据条件
    * 从tableVersionCacheMap查询
    * 对应的MysqlSchemaVersionCollection
    * 1.根据dbName获得对应的dbId
    * 2.获取dbId对应的tableVersionCache
    * 3.获取【当前条件】下的MysqlSchemaVersionCollection
    *
    * @param dbName
    * @param tableName
    * @param binlogPositionInfo
    * @return MysqlSchemaVersionCollection
    */
  private def getTableVersionInternal(dbName: String, tableName: String, binlogPositionInfo: BinlogPositionInfo): MysqlSchemaVersionCollection = {
    lazy val dbId = findDbId(dbId)
    Option(tableVersionCacheMap.get(dbName))
      .fold(throw new SchemaIsNotInitializedException(s"cannot find dbName :$dbName when get table version，pls check,id:$syncTaskId")) {
        map =>
          map.getCorespondingVersionSchema(tableName, binlogPositionInfo)
      }
  }

  /**
    * 主要逻辑在getTableVersionInternal中
    * 最后增加一步校验
    * 如果获取的Schema的字段数量和实际的对不上
    * 返回 -1 否则返回正确Version
    *
    * @param dbName
    * @param tableName
    * @param binlogPositionInfo
    * @param fieldCount
    * @return
    */
  def getTableVersion(dbName: String, tableName: String, binlogPositionInfo: BinlogPositionInfo, fieldCount: Int): Int = {
    lazy val re = getTableVersionInternal(dbName, tableName, binlogPositionInfo)
    if (re.fieldSize != fieldCount) {
      log.warn(s"this entry:${binlogPositionInfo} has ${re.fieldSize} field(s),which not equal to actual field count:$fieldCount,this entry should be abandoned");
      -1
    } else re.version
  }


  def upsertSchema(schemaEntryCollection: MysqlSchemaVersionCollection): Try[Unit] = {
     def isHandled()
    lazy val sql = convertSchemaVersionCollection2Sql(schemaEntryCollection)
    upsertSchemaIntoSchemaDatabase(sql)
      .map {
        _ =>


      }
  }

  /**
    * todo sql的设计
    *
    * @param sql
    * @return
    */
  def getSchemas(sql: => String): Try[TableSchemaVersionCache] = {

    ???
  }

  /**
    * 将变换的schema更新到数据库中
    *
    * @param sql
    * @return
    */
  private def upsertSchemaIntoSchemaDatabase(sql: => String): Try[Unit] = Try {
    val conn = mysqlConnection.fork
    conn.connect()
    conn.update(sql);
    conn.disconnect()
  }

  /**
    * 根据dbName寻找DbId
    *
    * @param dbName
    * @return
    */
  private def findDbId(dbName: String): String = Option(dbNameMappingdbIdMap.get(dbName))
    .fold(throw new SchemaIsNotInitializedException(s"cannot find dbId when findDbId at dbName:$dbName,pls check!,id:$syncTaskId"))(x => x)

  /**
    *
    * 将binlogPosition装换成Long
    * 方便比较
    *
    * @param binlogJournalName
    * @param binlogOffset
    * @return
    */
  private def convertBinlogPosition2Long(binlogJournalName: String, binlogOffset: Long) = s"${binlogJournalName.split('.')(1)}$binlogOffset".toLong

  /**
    * 将数据库query的行数据转换为实体
    *
    * @param row
    * @return
    */
  private def convertRow2ConSchemaEntry(
                                         row: Map[String, Any]

                                       ): MysqlConSchemaEntry = {
    //todo
    lazy val effectTimestamp = System.currentTimeMillis()
    lazy val dbName = row("dbName").toString
    lazy val dbId = row("dbId").toString
    lazy val tableName = row("tableName").toString
    lazy val tableId = row("tableId").toString
    lazy val tableComment = row("tableComment").toString
    lazy val fieldName = row("fieldName").toString
    lazy val fieldIndex = row("fieldIndex").toString.toInt
    lazy val fieldComment = row("fieldComment").toString
    lazy val fieldType = row("fieldType").toString
    lazy val fieldConstraint = row("fieldConstraint").toString
    lazy val timestamp = row("timestamp").toString.toLong * 1000
    lazy val version = row("version").toString.toInt
    lazy val binlogJournalName = row("binlogJournalName").toString
    lazy val binlogPosition = row("binlogPosition").toString.toLong
    lazy val isOriginal = if(row("isOriginal").toString.toInt == 1) true else false

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

  /**
    * 将MysqlSchemaVersionCollection转换成Sql
    * 要做成事务式提交!
    * TODO
    *
    * @param mysqlSchemaVersionCollection
    * @return
    */
  private def convertSchemaVersionCollection2Sql(mysqlSchemaVersionCollection: MysqlSchemaVersionCollection): String = {
    ???

  }
}


//
///**
//  * SELECT
//  *a.table_schema dbName,
//  *a.table_name tableName,             //表名
//  *a.table_comment tableComment,       //表注释
//  *a.create_time tableCreateTime       //表创建时间
//  *a.update_time tableUpdateTime       //表更新时间
//  *b.ordinal_position fieldIndex       //字段序号
//  *b.COLUMN_NAME fieldName,            //字段名称
//  *b.column_comment fieldComment,      //字段注释
//  *b.column_type  fieldType,           //字段类型
//  *b.column_key fieldConstraint        //字段约束
//  * FROM
//  * information_schema.TABLES a
//  * LEFT JOIN information_schema. COLUMNS b ON a.table_name = b.TABLE_NAME
//  */
//val sourceInfoSqlPrefix = "SELECT\na.table_name tableName,\na.table_comment tableComment,\nunix_timestamp(a.create_time) tableCreateTime,       \nunix_timestamp(a.update_time) tableUpdateTime,\nb.ordinal_position fieldIndex,\nb.COLUMN_NAME fieldName,\nb.column_comment fieldComment,\nb.column_type  fieldType,\nb.column_key fieldConstraint\nFROM\ninformation_schema. TABLES a\nLEFT JOIN information_schema. COLUMNS b ON a.table_name = b.TABLE_NAME"
