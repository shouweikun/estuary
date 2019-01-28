package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.mappings

import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{Column, RowData}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.MysqlRowDataInfo
import com.neighborhood.aka.laplace.estuary.mysql.schema.tablemeta.{EstuaryMysqlColumnInfo, MysqlTableSchemaHolder}
import com.neighborhood.aka.laplace.estuary.mysql.utils.CanalEntryTransHelper

import scala.collection.JavaConverters._
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
    schemaHolder
      .flatMap(_.getTableMetaByFullName(s"$dbName.$tableName"))
      .map {
        tableMeta =>
          columnList.forall(x => tableMeta.columnInfoMap.contains(x.name))
      }.getOrElse(false)
  }

}