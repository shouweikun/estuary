package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.DdlType.DdlType

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
                                  val hbaseIndex: Int,
                                  val fieldName: String,
                                  val fieldComment: String,
                                  val fieldType: String,
                                  val trickleFieldType: String,
                                  //暂时没用上
                                  val fieldConstraint: String,
                                  val version: Int,
                                  val binlogPositionInfo: BinlogPositionInfo,
                                  //是否是初始
                                  val isOriginal: Boolean = false,
                                  val isDeleted: Boolean = false,
                                  val isKey: Boolean = false,
                                  val ddlSql: String = "",
                                  val tableComment: String = "",
                                  val lastTableId: String = "",
                                  val ddlType: DdlType = DdlType.UNKNOWN,
                                  val defaultValue: String = ""

                                ) extends SchemaEntry {

    //每次新加字段都在这里更新一下
    lazy val fieldList: List[String] = List(schemaName, tableName, hbaseIndex, fieldName, fieldComment, fieldType, fieldConstraint, version, binlogPositionInfo, isOriginal, isDeleted, isKey, ddlSql, tableComment, lastTableId, ddlType, defaultValue).map {
      x =>
        x match {
          case str: String => s"'$x'"
          case _ => x.toString
        }
    }

    override def toString: String = {
      s"""
         |  schemaId:$schemaId,
         |  schemaName:$schemaName,
         |  tableId:$tableId,
         |  tableName:$tableName,
         |  hbaseIndex:$hbaseIndex,
         |  fieldName:$fieldName,
         |  fieldComment:$fieldComment,
         |  fieldType:$fieldType
         |  trickleFieldType:$trickleFieldType,
         |  fieldConstraint:$fieldConstraint,
         |  version:$version,
         |  binlogPositionInfo:$binlogPositionInfo,
         |  isOriginal:$isOriginal
         |  isDeleted:$isDeleted
         |  isKey:$isKey
         |  ddlSql:$ddlSql
         |  tableComment:$tableComment,
         |  lastTableId: $lastTableId,
         |  ddlType:$ddlType,
         |  defaultValue: $defaultValue
       """.stripMargin
    }

    def markNotDeleted(ddlSql: String = this.ddlSql, fieldComment: String = this.fieldComment): MysqlConSchemaEntry = MysqlConSchemaEntry(
      schemaId,
      schemaName,
      tableId,
      tableName,
      hbaseIndex,
      fieldName,
      fieldComment,
      fieldType,
      trickleFieldType,
      fieldConstraint,
      version,
      binlogPositionInfo,
      //是否是初始
      isOriginal,
      false,
      isKey,
      ddlSql,
      tableComment,
      lastTableId,
      ddlType,
      defaultValue
    )

    def markAsDeleted(ddlSql: String = this.ddlSql): MysqlConSchemaEntry = MysqlConSchemaEntry(
      schemaId,
      schemaName,
      tableId,
      tableName,
      hbaseIndex,
      fieldName,
      fieldComment,
      fieldType,
      trickleFieldType,
      fieldConstraint,
      version,
      binlogPositionInfo,
      //是否是初始
      isOriginal,
      true,
      isKey,
      ddlSql,
      tableComment,
      lastTableId,
      ddlType,
      defaultValue
    )

    def changeColumnAttributes(newColumnName: String, newColumnType: String, newColumnComment: String): MysqlConSchemaEntry = {

      SchemaEntry.generateMysqlConSchemaEntry(
        schemaId: String,
        schemaName: String,
        tableId: String,
        tableName: String,
        hbaseIndex: Int,
        if (newColumnName.trim == "") this.fieldName else newColumnName,
        if (newColumnComment.trim == "") this.fieldComment else newColumnComment,
        if (newColumnType.trim == "") this.fieldType else newColumnType,
        fieldConstraint: String,
        version: Int,
        binlogPositionInfo: BinlogPositionInfo,
        //是否是初始
        isOriginal: Boolean,
        isDeleted: Boolean,
        isKey,
        ddlSql,
        tableComment,
        lastTableId,
        ddlType,
        defaultValue
      )
    }

    def updateForNewVersion(
                             tableName: String,
                             ddlSql: String,
                             version: Int,
                             isOriginal: Boolean,
                             binlogPositionInfo: BinlogPositionInfo
                           ): MysqlConSchemaEntry = {
      SchemaEntry.generateMysqlConSchemaEntry(
        schemaId.toLowerCase,
        schemaName.toLowerCase,
        tableId.toLowerCase,
        tableName.toLowerCase,
        hbaseIndex,
        fieldName,
        fieldComment,
        fieldType,
        fieldConstraint,
        version,
        binlogPositionInfo: BinlogPositionInfo,
        //是否是初始
        false,
        isDeleted,
        isKey,
        ddlSql,
        tableComment,
        lastTableId,
        ddlType,
        defaultValue
      )
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

  def generateMysqlConSchemaEntry(
                                   schemaId: String,
                                   schemaName: String,
                                   tableId: String,
                                   tableName: String,
                                   hbaseIndex: Int,
                                   fieldName: String,
                                   fieldComment: String,
                                   fieldType: String,
                                   fieldConstraint: String,
                                   version: Int,
                                   binlogPositionInfo: BinlogPositionInfo,
                                   //是否是初始
                                   isOriginal: Boolean = false,
                                   isDeleted: Boolean = false,
                                   isKey: Boolean = false,
                                   ddlSql: String = "",
                                   tableComment: String = "",
                                   lastTableId: String = "",
                                   ddlType: DdlType = DdlType.UNKNOWN,
                                   defaultValue: String = ""
                                 ): MysqlConSchemaEntry = new MysqlConSchemaEntry(
    schemaId.toLowerCase: String,
    schemaName.toLowerCase: String,
    tableId.toLowerCase: String,
    tableName.toLowerCase: String,
    hbaseIndex: Int,
    fieldName: String,
    fieldComment: String,
    fieldType: String,
    toJavaType(fieldType),
    fieldConstraint: String,
    version: Int,
    binlogPositionInfo: BinlogPositionInfo,
    //是否是初始
    isOriginal: Boolean,
    isDeleted: Boolean,
    isKey,
    ddlSql: String,
    lastTableId = lastTableId,
    ddlType = ddlType,
    defaultValue = defaultValue,
    tableComment = tableComment
  )

  private def toJavaType(t: String) = t.toLowerCase.split("\\(")(0) match {
    case "varchar" => "string"
    case "char" => "string"
    case "tinytext" => "string"
    case "mediumtext" => "string"
    case "longtext" => "string"
    case "text" => "string"
    case "datetime" => "timestamp"
    case "timestamp" => "timestamp"
    case "date" => "string"
    case "set" => "string"
    case "enum" => "string"
    case "time" => "string"
    case "tinyint" => "bigint"
    case "smallint" => "int"
    case "bigint" => "bigint"
    case "mediumint" => "bigint"
    case "int" => "int"
    case "float" => t
    case "decimal" => t
    case "bit" => "int"
    case "double" => t
    case _ => "STRING"
  }
}


