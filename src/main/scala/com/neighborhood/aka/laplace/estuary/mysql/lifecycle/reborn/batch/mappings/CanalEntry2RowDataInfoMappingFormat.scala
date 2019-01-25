package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.mappings

import com.alibaba.otter.canal.protocol.CanalEntry.EventType
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{BinlogPositionInfo, MysqlRowDataInfo}
import com.neighborhood.aka.laplace.estuary.mysql.schema.tablemeta.MysqlTableSchemaHolder

/**
  * Created by john_liu on 2019/1/13.
  */
trait CanalEntry2RowDataInfoMappingFormat extends CanalEntryMappingFormat[MysqlRowDataInfo] {

  /**
    * 是否检查Schema
    */
  def isCheckSchema: Boolean

  /**
    *  schema缓存
    */
  def schemaHolder: Option[MysqlTableSchemaHolder] = None

  override def transform(x: lifecycle.EntryKeyClassifier): MysqlRowDataInfo = {
    val entry = x.entry
    val tableName = entry.getHeader.getTableName
    val dbName = entry.getHeader.getSchemaName

    val sql: String = entry.getHeader.getEventType
    match {
      case EventType.INSERT | EventType.UPDATE => handleUpdateEventRowDataToSql(dbName, tableName, x.rowData)
      case EventType.DELETE => handleDeleteEventRowDataToSql(dbName, tableName, x.rowData)
    }
    val binlogPositionInfo = BinlogPositionInfo(
      entry.getHeader.getLogfileName,
      entry.getHeader.getLogfileOffset,
      entry.getHeader.getExecuteTime
    )
    MysqlRowDataInfo(x.entry.getHeader.getSchemaName, x.entry.getHeader.getTableName, x.entry.getHeader.getEventType, x.rowData, binlogPositionInfo, Option(sql))
  }

  /**
    * 处理Mysql的UpdateEvent和InsertEvent
    *
    * @param dbName    数据库名称
    * @param tableName 表名称
    * @param rowData   rowData
    * @return Sql
    */
  private def handleUpdateEventRowDataToSql(dbName: String, tableName: String, rowData: RowData): String = {
    val values = new ListBuffer[String]
    val fields = new ListBuffer[String]
    (0 until rowData.getAfterColumnsCount).foreach {
      index =>
        val column = rowData.getAfterColumns(index)
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
    * @param rowData   rowData
    * @return sql
    */

  private def handleDeleteEventRowDataToSql(dbName: String, tableName: String, rowData: RowData): String = {
    val columnList = rowData.getBeforeColumnsList.asScala
    columnList.find(x => x.hasIsKey && x.getIsKey).fold { //暂时不处理无主键的
      ""
    } {
      keyColumn =>
        val keyName = keyColumn.getName
        val keyValue = CanalEntryTransHelper.getSqlValueByMysqlType(keyColumn.getMysqlType, keyColumn.getValue)
        s"DELETE FROM $dbName.$tableName WHERE $keyName=$keyValue"
    }
  }
}