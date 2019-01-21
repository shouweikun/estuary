package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import java.util.UUID

import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.TableSchemaVersionCache.MysqlSchemaVersionCollection
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2018/6/23.
  */
class SqlBuilder(
                  taskInfoManager: MysqlSourceManagerImp with TaskManager
                ) {
  /**
    * 同步任务Id
    */
  lazy val syncTaskId = taskInfoManager.syncTaskId
  /**
    * 日志
    */
  lazy val logger = LoggerFactory.getLogger(s"SqlBuilder-$syncTaskId")
  /**
    * 配置
    */
  lazy val config = taskInfoManager.config
  /**
    * schema数据库名
    */
  val schemaDbName = if (config.hasPath("schema.db")) config.getString("schema.db") else ""
  /**
    * 表字段信息 表名
    */
  val fieldInfoTableName = s"${schemaDbName}.trickle3_schema_field_info"
  /**
    * 数据库信息 表名
    */
  val databaseSchemaTableName = s"${schemaDbName}.trickle3_schema_dbs"
  /**
    * 表信息 表名
    */
  val tableSchemaTableName = s"${schemaDbName}.trickle3_schema_tables"
  /**
    * 表版本 表名
    */
  val schemaVersionTableName = s"${schemaDbName}.trickle3_schema_version"
  /**
    * HBase-Hive映射 表名
    */
  val schemaMappingTableName = s"${schemaDbName}.trickle3_schema_hbase_hive_mapping"

  /**
    * 将MysqlSchemaVersionCollection转换成SqlPrefixParamList
    *
    * @param mysqlSchemaVersionCollection
    * @return
    */
  def transferMysqlSchemaVersionCollection2SqlPrefixParamList(mysqlSchemaVersionCollection: => MysqlSchemaVersionCollection): List[(String, List[(String, Any)])] = {
    lazy val dbId = mysqlSchemaVersionCollection.dbId
    lazy val dbName = mysqlSchemaVersionCollection.dbName
    lazy val version = mysqlSchemaVersionCollection.version
    lazy val tableComment = mysqlSchemaVersionCollection.tableComment
    //使用UUID保证全局唯一
    lazy val versionId = UUID.randomUUID().toString()
    lazy val tableId = mysqlSchemaVersionCollection.tableId
    lazy val tableName = mysqlSchemaVersionCollection.tableName
    lazy val binlogPositionInfo = mysqlSchemaVersionCollection.binlogInfo
    lazy val binlogTimestamp = binlogPositionInfo.timestamp
    lazy val binlogJournalName = binlogPositionInfo.journalName
    lazy val binlogOffset = binlogPositionInfo.offset
    lazy val lastTableId = mysqlSchemaVersionCollection.lastTableId
    lazy val ddlType = DdlType.ddlTypetoString(mysqlSchemaVersionCollection.ddlType)
    lazy val schemas = mysqlSchemaVersionCollection.schemas
    lazy val ddlSql =
      s"""${mysqlSchemaVersionCollection.ddlSql}""".stripMargin
    lazy val isOriginal = schemas(0).isOriginal
    /**
      * mapping表
      */
    lazy val schemaMappingTableSqlPrefix = s"INSERT INTO $schemaMappingTableName (`hive_table_name`,`db_name`,`trickle3_schema_hbase_dbs_name`, `trickle3_schema_version_id`,`trickle3_schema_hbase_unique_table_name`,`hive_view_name`, `comment`,`status`) VALUES (?,?,?,?,?,?,'no_comment','normal');"

    lazy val schemaMappingTableParamList = List("string", "todo", ("string", dbName), ("string", dbId), ("string", versionId), ("string", tableId), ("string", tableName))
    /**
      * version表
      */
    lazy val schemaVersionTableSqlPrefix =
      s"""INSERT INTO $schemaVersionTableName (`ddl_type`,`last_trickle3_schema_hbase_unique_table_name`,`trickle3_schema_hbase_unique_table_name`,`trickle3_schema_version_id` ,`version` ,`timestamp` ,  `mysql_binlog_journal_name`,`mysql_binlog_position`,`is_original`,`comment`,`alter_sql`) VALUES (?,?,?,?,?,?,?,?,0,?,?);"""
    /**
      * todo 这块做的不好，binlog中的时间戳是ms，在这里需要进行处理
      * 尝试之后用jdbc来替换
      */
    lazy val schemaVersionTableParamList = List(("string", ddlType), ("string", lastTableId), ("string", tableId), ("string", versionId), ("int", version), ("long", binlogTimestamp / 1000), ("string", binlogJournalName), ("long", binlogOffset), ("string", tableComment), ("string", ddlSql))
    /**
      * 如果是Original，发起一次table表中的建表
      * 可能是create 可能是truncate，可能是rename
      */
    lazy val tableSchemaTableNameSqlPrefix = s"INSERT INTO $tableSchemaTableName (`trickle3_schema_hbase_dbs_name`,`trickle3_schema_hbase_unique_table_name`,`comment`) VALUES (?,?,'no_comment')"
    lazy val tableSchemaTableNameParamList = List(("string", dbId), ("string", tableId))
    /**
      * 字段表
      */
    lazy val schemaFieldTableSqlPrefix = s"INSERT INTO $fieldInfoTableName (`default_value`,`is_deleted`,`trickle3_schema_hbase_unique_table_name`, `field_name`, `field_source_data_type`, `field_data_type`, `trickle3_schema_version_id`,`index`,`comment`,`is_primary_key`) VALUES (?,?,?,?,?,?,?,?,?,?);"
    lazy val schemaFieldTableSqlList = schemas.map {
      schema =>

        lazy val fieldName = schema.fieldName
        lazy val sourceFieldType = schema.fieldType
        lazy val fieldType = schema.trickleFieldType
        lazy val index = schema.hbaseIndex
        lazy val comment = schema.fieldComment
        lazy val key = if (schema.isKey) "PRI" else ""
        lazy val isDeleted = if (schema.isDeleted) "true" else "false"
        lazy val defaultValue = schema.defaultValue
        lazy val paramList = List(("string", defaultValue), ("string", isDeleted), ("string", tableId), ("string", fieldName), ("string", sourceFieldType), ("string", fieldType), ("string", versionId), ("int", index), ("string", comment), ("string", key))

        (schemaFieldTableSqlPrefix, paramList)
    }

    lazy val baseSqlList = schemaFieldTableSqlList
      .::(schemaMappingTableSqlPrefix, schemaMappingTableParamList)
      .::(schemaVersionTableSqlPrefix, schemaVersionTableParamList)
//
//    if (isOriginal) baseSqlList.::(tableSchemaTableNameSqlPrefix, tableSchemaTableNameParamList) else baseSqlList
    ???
  }

  /**
    * 构建查询元数据信息的sql语句
    *
    * @param dbName
    * @return
    */
  def getSchemaSql(dbName: String) =
    s"""select
                           mapping.db_name dbName,
                           mapping.hive_view_name tableName,
                           mapping.`trickle3_schema_hbase_dbs_name` dbId,
                           mapping.`trickle3_schema_hbase_unique_table_name` tableId,
                           version.version version,
                           version.`is_original` isOriginal,
                           version.mysql_binlog_journal_name binlogJournalName,
                           version.mysql_binlog_position binlogPosition,
                           version.timestamp timestamp,
                           version.`last_trickle3_schema_hbase_unique_table_name` lastTableId,
                           version.comment tableComment,
                           version.ddl_type ddlType,
                           version.alter_sql ddlSql,
                           field.`field_name` fieldName,
                           field.index hbaseIndex,
                           field.`field_source_data_type` fieldType,
                           field.`field_data_type` trickleFieldType,
                           field.`comment` fieldComment,
                           field.is_primary_key isKey,
                           field.is_deleted isDeleted,
                           field.default_value defaultValue
                        from
                       $fieldInfoTableName field
                       left join
                       (
                       select
                       	db_name,
                       	hive_view_name,
                       	trickle3_schema_hbase_dbs_name,
                       	trickle3_schema_hbase_unique_table_name,
                       	trickle3_schema_version_id
                       	from
                       		$schemaMappingTableName
                       	where
                       		db_name="$dbName"
                       		and status="normal"
                       ) mapping
                       on
                       field.trickle3_schema_version_id=mapping.trickle3_schema_version_id
                       left join
                       $schemaVersionTableName version
                       on
                       	field.trickle3_schema_version_id=version.trickle3_schema_version_id
                      where mapping.db_name="$dbName"  """;


}
