package com.neighborhood.aka.laplace.estuary.mysql.schema

import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.mysql.schema.defs.columndef.ColumnDef

/**
  * Created by john_liu on 2019/1/23.
  */
package object tablemeta {

  sealed trait ColumnInfo

  /**
    * 入海口的MysqlColumnInfo定义
    *
    * @param name      列名称
    * @param index     索引
    * @param mysqlType mysqlType
    */
  final case class EstuaryMysqlColumnInfo(name: String, index: Int, mysqlType: String) extends ColumnInfo

  /**
    * 入海口的Mysql表定义
    *
    * @param schemaName 库名称
    * @param tableName  表名称
    * @param columns    列
    */
  final case class EstuaryMysqlTableMeta(schemaName: String, tableName: String, columns: List[EstuaryMysqlColumnInfo],   createTableSql: Option[String] = None) {
    val columnNum = columns.size
    val columnInfoMap = columns.map(x => (x.name -> x)).toMap
  }


  implicit final class ColumnDefEstuaryMysqlColumnInfoSyntax(column: ColumnDef) {
    def toEstuaryMysqlColumnInfo: EstuaryMysqlColumnInfo = {
      EstuaryMysqlColumnInfo(column.getName, column.getPos, column.getType)
    }
  }

  implicit final class CanalColumnEstuaryMysqlColumnInfoSyntax(column: CanalEntry.Column) {
    def toEstuaryMysqlColumnInfo: EstuaryMysqlColumnInfo = {
      EstuaryMysqlColumnInfo(name = column.getName, index = column.getIndex, mysqlType = column.getMysqlType)
    }
  }

}
