package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.mappings

import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{EventType, RowData}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{BinlogPositionInfo, MysqlRowDataInfo}
import com.neighborhood.aka.laplace.estuary.mysql.schema.tablemeta.{EstuaryMysqlColumnInfo, MysqlTableSchemaHolder, _}
import com.neighborhood.aka.laplace.estuary.mysql.utils.CanalEntryTransHelper

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by john_liu on 2019/1/13.
  */
trait CanalEntry2RowDataInfoMappingFormat extends CanalEntryMappingFormat[MysqlRowDataInfo] {

  /**
    * 是否检查Schema
    */
  def isCheckSchema: Boolean

  /**
    * schema缓存
    */
  def schemaHolder: Option[MysqlTableSchemaHolder] = None

  /**
    * 处理Mysql的UpdateEvent和InsertEvent
    *
    * @param dbName     数据库名称
    * @param tableName  表名称
    * @param columnList List[EstuaryMysqlColumnInfo]
    * @return Sql
    */
  @inline
  protected def handleUpdateEventRowDataToSql(dbName: String, tableName: String, columnList: List[CanalEntry.Column]): String = {
    val values = new ListBuffer[String]
    val fields = new ListBuffer[String]
    columnList.foreach {
      column =>
        if (column.hasValue) {
          values.append(CanalEntryTransHelper.getSqlValueByMysqlType(column.getMysqlType, column.getValue))
          fields.append(column.getName)
        }
    }
    s"replace into $dbName.$tableName(${fields.mkString(",")}) VALUES (${values.mkString(",")}) "
  }

  /**
    * 处理Mysql的Delete事件
    *
    * @param dbName    数据库名称
    * @param tableName 表名称
    * @return sql
    */
  @inline
  protected def handleDeleteEventRowDataToSql(dbName: String, tableName: String, columnList: List[CanalEntry.Column]): String = {
    columnList.find(x => x.hasIsKey && x.getIsKey).fold { //暂时不处理无主键的
      ""
    } {
      keyColumn =>
        val keyName = keyColumn.getName
        val keyValue = CanalEntryTransHelper.getSqlValueByMysqlType(keyColumn.getMysqlType, keyColumn.getValue)
        s"DELETE FROM $dbName.$tableName WHERE $keyName=$keyValue"
    }
  }

  /**
    * 检测Schema
    *
    * @param dbName    数据库名称
    * @param tableName 表名称
    * @param columnList
    * @return
    */
  protected def checkSchema(dbName: String, tableName: String, columnList: List[EstuaryMysqlColumnInfo]): Boolean = {
    if (!isCheckSchema) true
    else schemaHolder
      .flatMap(_.getTableMetaByFullName(s"$dbName.$tableName"))
      .map { tableMeta =>
        columnList
          .forall(x => tableMeta.columnInfoMap.contains(x.name))
      }
      .getOrElse(false)
  }

  /**
    * 检查并获取RowDataInfo
    *
    * @param dbName    db名称
    * @param tableName 表名称
    * @param dmlType   dml类型
    * @param rowData   rowData
    * @return MysqlRowDataInfo
    */
  @inline
  protected def checkAndGetMysqlRowDataInfo(
                                             dbName: String,
                                             tableName: String,
                                             dmlType: EventType,
                                             rowData: RowData,
                                             entry: CanalEntry.Entry
                                           ): MysqlRowDataInfo = {
    lazy val columnList: List[CanalEntry.Column] = {
      val buffer = dmlType match {
        case EventType.DELETE => rowData.getBeforeColumnsList.asScala
        case EventType.INSERT | EventType.UPDATE => rowData.getAfterColumnsList.asScala
        case _ => mutable.Buffer.empty
      }
      buffer.toList
    }
    //EstuaryMysqlColumnInfo的list 方便值比较
    val estuaryColumnInfoList = columnList.map(_.toEstuaryMysqlColumnInfo)
    val sql: String = if (!checkSchema(dbName, tableName, estuaryColumnInfoList)) {
      logger.warn(s"check schema failed,entry:${CanalEntryTransHelper.entryToJson(entry)},id:$syncTaskId")
      "" //返回空字符串
    }
    else dmlType match {
      case EventType.INSERT | EventType.UPDATE => handleUpdateEventRowDataToSql(dbName, tableName, columnList)
      case EventType.DELETE => handleDeleteEventRowDataToSql(dbName, tableName, columnList)
    }
    val binlogPositionInfo = BinlogPositionInfo(
      entry.getHeader.getLogfileName,
      entry.getHeader.getLogfileOffset,
      entry.getHeader.getExecuteTime
    )
    MysqlRowDataInfo(dbName, tableName, dmlType, rowData, binlogPositionInfo, Option(sql))
  }
}