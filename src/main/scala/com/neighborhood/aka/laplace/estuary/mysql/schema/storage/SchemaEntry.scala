package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

/**
  * Created by john_liu on 2018/5/31.
  *
  * @todo
  */
sealed trait SchemaEntry


object SchemaEntry {

  class EmptySchemaEntry extends SchemaEntry

  case class MysqlConSchemaEntry(
                                  val schemaId: String,
                                  val schemaName: String,
                                  val tableId: String,
                                  val tableName: String,
                                  val tableComment: String,
                                  val fieldPositionIndex: Int,
                                  val fieldName: String,
                                  val fieldComment: String,
                                  val fieldType: String,
                                  val fieldConstraint: String,
                                  var version: Long,
                                  var timestamp: Long,
                                  var binlogFileName: String,
                                  var binlogPosition: Long,
                                  //是否是初始
                                  var isOriginal: Boolean = false,
                                  val ddlSql: String = ""
                                ) extends SchemaEntry {

    //每次新加字段都在这里更新一下
    lazy val fieldList: List[String] = List(schemaName, tableName, tableComment, fieldPositionIndex, fieldName, fieldComment, fieldType, fieldConstraint, version, timestamp, binlogFileName, binlogPosition).map {
      x =>
        x match {
          case str: String => s"'$x'"
          case _ => x.toString
        }
    }

    /**
      *
      * @param targetSchemaName 给定的元数据信息数据库
      * @param targetTableName  表名
      * @return
      */
    @deprecated
    def convertEntry2Sql(targetSchemaName: String, targetTableName: String): String = s"INSERT INTO $targetSchemaName.$targetTableName VALUES(${fieldList mkString (",")})"
  }

}


