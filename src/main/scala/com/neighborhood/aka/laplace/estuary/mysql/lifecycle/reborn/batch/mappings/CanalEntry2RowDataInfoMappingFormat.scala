package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.mappings

import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.EventType
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{BinlogPositionInfo, MysqlRowDataInfo}
import com.neighborhood.aka.laplace.estuary.mysql.schema.tablemeta.{EstuaryMysqlColumnInfo, MysqlTableSchemaHolder, _}
import com.neighborhood.aka.laplace.estuary.mysql.utils.CanalEntryTransHelper

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/**
  * Created by john_liu on 2019/1/13.
  */
trait CanalEntry2RowDataInfoMappingFormat extends CanalEntryMappingFormat[MysqlRowDataInfo] {

  sealed protected case class OperationField(
                                              dbName: String,
                                              tableName: String,
                                              column: CanalEntry.Column,
                                              value: String,
                                              entry: CanalEntry.Entry)

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
  protected def handleUpdateEventRowDataToSql(dbName: String, tableName: String, columnList: List[CanalEntry.Column], entry: => CanalEntry.Entry): List[String] = {
    val values = new ListBuffer[String]
    val fields = new ListBuffer[String]
    columnList.foreach {
      column =>
        if (!column.getIsNull && column.hasValue) {
          val value = mapRowValue((OperationField(dbName, tableName, column, column.getValue, entry)))
          values.append(value)
          fields.append(column.getName)
        }
    }
    //    val delete = handleDeleteEventRowDataToSql(dbName, tableName, columnList, entry).head
    val insert = s"replace into `$dbName`.`$tableName`(${fields.mkString(",")}) VALUES (${values.mkString(",")}) "
    List(
      //      delete,
      insert)
  }

  /**
    * 处理Mysql的Delete事件
    *
    * @param dbName    数据库名称
    * @param tableName 表名称
    * @return sql
    */
  @inline
  protected def handleDeleteEventRowDataToSql(dbName: String, tableName: String, columnList: List[CanalEntry.Column], entry: CanalEntry.Entry): List[String] = {
    columnList.find(x => x.hasIsKey && x.getIsKey && x.hasValue).fold { //暂时不处理无主键的
      List.empty[String]
    } {
      keyColumn =>
        val keyName = keyColumn.getName
        val keyValue = (mapRowValue((OperationField(dbName, tableName, keyColumn, keyColumn.getValue, entry))))
        List(s"DELETE FROM `$dbName`.`$tableName` WHERE $keyName=$keyValue")
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
    * @param columnList
    * @param entry
    * @return MysqlRowDataInfo
    */
  @inline
  protected def checkAndGetMysqlRowDataInfo(
                                             dbName: String,
                                             tableName: String,
                                             dmlType: EventType,
                                             columnList: List[CanalEntry.Column],
                                             entry: => CanalEntry.Entry
                                           ): MysqlRowDataInfo = {

    //EstuaryMysqlColumnInfo的list 方便值比较
    val estuaryColumnInfoList = columnList.map(_.toEstuaryMysqlColumnInfo)
    val sql: List[String] = if (!checkSchema(dbName, tableName, estuaryColumnInfoList)) {
      logger.warn(s"check schema failed,entry:${CanalEntryTransHelper.entryToJson(entry)},id:$syncTaskId")
      List.empty //返回空字符串
    }
    else dmlType match {
      case EventType.INSERT | EventType.UPDATE => handleUpdateEventRowDataToSql(dbName, tableName, columnList, entry)
      case EventType.DELETE => handleDeleteEventRowDataToSql(dbName, tableName, columnList, entry)
      case _ => throw new UnsupportedOperationException(s"$dmlType is not supported currently,id:$syncTaskId")
    }
    val binlogPositionInfo = BinlogPositionInfo(
      entry.getHeader.getLogfileName,
      entry.getHeader.getLogfileOffset,
      entry.getHeader.getExecuteTime
    )
    MysqlRowDataInfo(dbName, tableName, dmlType, columnList, binlogPositionInfo, sql)
  }

  /**
    * 处理每行的值
    *
    * @param input    输入
    * @param funcList 变换方法列表
    * @return
    */
  @tailrec
  protected final def mapRowValue(input: => OperationField, funcList: List[PartialFunction[OperationField, OperationField]] = this.valueMappingFunctions): String = {
    funcList match {
      case f :: nextFuncList => {
        lazy val nextInput = f.applyOrElse(input, (input: OperationField) => input)
        mapRowValue(nextInput, nextFuncList)
      }
      case Nil => input.value
    }
  }

  /**
    * 所有的值变换策略
    *
    * @note 请使用:: 来添加新方法
    */
  protected val valueMappingFunctions: List[PartialFunction[OperationField, OperationField]] = List(getSqlValueBySqlType)

  /**
    * 根据sqltype 来决定是否加`'``'`
    *
    **/
  protected val getSqlValueBySqlType = new PartialFunction[OperationField, OperationField] {
    override def isDefinedAt(x: OperationField): Boolean = true

    override def apply(v1: OperationField): OperationField = v1.copy(value = CanalEntryTransHelper.getSqlValueByMysqlType(v1.column.getMysqlType, v1.value))

  }

}