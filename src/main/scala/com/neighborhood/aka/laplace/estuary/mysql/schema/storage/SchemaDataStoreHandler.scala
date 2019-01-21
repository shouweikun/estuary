package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import java.net.InetSocketAddress

import com.neighborhood.aka.laplace.estuary.bean.exception.schema.CannotFindTableCommentException
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.source.{MysqlConnection, MysqlSourceManagerImp}
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2018/6/23.
  */
class SchemaDataStoreHandler(
                              taskInfoManager:   MysqlSourceManagerImp with TaskManager
                            ) {
  /**
    * 同步任务Id
    */
  lazy val syncTaskId = taskInfoManager.syncTaskId
  /**
    * 日志
    */
  lazy val logger = LoggerFactory.getLogger(s"SchemaDataStoreHandler-$syncTaskId")
  /**
    * 配置
    */
  lazy val config = taskInfoManager.config
  /**
    * 元数据数据库的配置
    */
  val ip: String = config.getString("schema.ip")
  val port: Int = config.getInt("schema.port")
  val user: String = config.getString("schema.user")
  val password: String = config.getString("schema.password")
  /**
    * 数据库连接
    */
  lazy val mysqlConnection = new MysqlConnection(
    address = new InetSocketAddress(ip, port),
    username = user,
    password = password
  )
  /**
    * Jdbc Connection
    */
  lazy val jdbcMysqlConnection = mysqlConnection.toJdbcConnecton
  lazy val sourceJdbcMysqlConnection = taskInfoManager.source.fork.toJdbcConnecton
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
    * SELECT
    * db_name,
    * trickle3_schema_dbs_id,
    * FROM
    * $schemaDbName.$databaseSchemaTableName
    * WHERE
    * db_name  = '$dbName'
    *
    * 查询元数据库来判断改数据库是否被初始化
    * * @param dbName
    *
    * @return
    */
  def getInitializedDbId(dbName: String): List[String] = {
    lazy val sql = s"SELECT trickle3_schema_hbase_dbs_name dbId FROM $databaseSchemaTableName WHERE db_name  = '$dbName' and source_type = 'mysql'"
    val conn = mysqlConnection.fork
    conn.connect()
    val list = {
      val re = conn.queryForScalaList(sql)
      conn.disconnect()
      re
    }
    list.map(_ ("dbId").toString)
  }

  /**
    * 使用binlog判断该任务是否执行过
    *
    * @param binlogInfo
    * @return
    */
  def isExistedOperation(binlogInfo: BinlogPositionInfo): Boolean = {


    lazy val sql = s"select count(*) from $schemaVersionTableName where mysql_binlog_journal_name = '${binlogInfo.journalName}' and mysql_binlog_position = ${binlogInfo.offset}"
    lazy val conn = jdbcMysqlConnection.fork
    try {
      conn.selectSql(sql).nonEmpty
    } catch {
      case e: Throwable => throw e
    } finally conn.disconnect()
  }

  /**
    * 将给定的sqlList更新回数据库
    * 发起事务
    *
    * @param sqlList
    * @return
    */
  def updateSqlListIntoDatabase(sqlList: List[(String, List[(String, Any)])]): Boolean = {
    lazy val conn = jdbcMysqlConnection.fork
    try {
      conn.startTransaction
      val isSuccess = sqlList.map {
        case (sqlPrefix, paramList) =>
          conn.insertPrepareSql(sqlPrefix, paramList)
      }.forall(_.equals(1))
      logger.info(s"insertPrepareSql done,upsert compelete,id:$syncTaskId")
      conn.commit
      isSuccess
    } catch {
      case e: Throwable => e.printStackTrace(); false
    } finally {
      conn.disconnect()
    }
  }

  /**
    * 删除信息的具体实现
    * 调用trickle-util中的现成方法
    *
    * @param dbId
    * @param tableId
    */
  def deleteInfoFromDatabase(dbId: String, tableId: String): Unit = {
    logger.info(s"start delete Info From Database table:$dbId.$tableId")
//    InitSchema.deleteUniqueTableAllSchema(tableId, config, dbId)
    logger.info(s"finish delete Info From Database table:$dbId.$tableId")
  }

  /**
    * 执行sql
    * 使用MysqlConnection
    *
    * @param sql
    * @return
    */
  def executeOneSqlQueryByMysqlConnection(sql: String): List[Map[String, Any]] = {
    val conn = mysqlConnection.fork
    try {
      conn.connect()
      conn.queryForScalaList(sql)
    } catch {
      case e: Throwable => throw e
    }
    finally conn.disconnect()
  }

  /**
    * 从数据源数据库中查询出表字段的comment
    *
    * @param dbName    数据库名称
    * @param tableName 表名称
    * @return
    */
  @throws[Exception]
  def getFieldCommentBySourceMysqlConnection(dbName: String, tableName: String): Map[String, String] = {
    try {
      if (!sourceJdbcMysqlConnection.isConnected) sourceJdbcMysqlConnection.connect()
      val sql =
        s"""select COLUMN_COMMENT comment,COLUMN_NAME name
           | from INFORMATION_SCHEMA.COLUMNS WHERE  LOWER(TABLE_SCHEMA) = '${dbName.toLowerCase}' AND  LOWER(TABLE_NAME) = '${tableName.toLowerCase}'""".stripMargin //注意要转成小写
      logger.info(s"start to query FieldComment from source,dbName:$dbName,tableName:$tableName")
      sourceJdbcMysqlConnection
        .selectSql(sql)
        .map {
          rowMap =>
            (rowMap.get("column_name").get.toString.toLowerCase -> rowMap.get("column_comment").getOrElse("").toString)
        }
        .toMap
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        throw e
      }
    }
    finally {
      sourceJdbcMysqlConnection.disconnect()
    }
  }


  def getTableCommentBySourceMysqlConnection(dbName: String, tableName: String): String = {
    try {
      if (!sourceJdbcMysqlConnection.isConnected) sourceJdbcMysqlConnection.connect()
      val sql =
        s"""select TABLE_COMMENT from INFORMATION_SCHEMA.TABLES
 WHERE  LOWER(TABLE_SCHEMA) = '${dbName.toLowerCase}' AND  LOWER(TABLE_NAME) = '${tableName.toLowerCase}'""".stripMargin //注意要转成小写
      logger.info(s"start to query TableComment from source,dbName:$dbName,tableName:$tableName")
      sourceJdbcMysqlConnection
        .selectSql(sql) match {
        case head :: Nil => head.get("TABLE_COMMENT").map(_.toString).getOrElse(throw new CannotFindTableCommentException(s"cannot find table comment,:$dbName.$tableName"))
        case Nil => throw new CannotFindTableCommentException(s"cannot find table comment,:$dbName.$tableName")
        case x => throw new CannotFindTableCommentException(s"cannot find table comment,cause there is more than one comment:$x,:$dbName.$tableName")
      }

    } catch {
      case e: Throwable => {
        e.printStackTrace()
        //不想上抛异常，只打印异常栈
        ""
      }
    }
    finally {
      sourceJdbcMysqlConnection.disconnect()
    }
  }

}
