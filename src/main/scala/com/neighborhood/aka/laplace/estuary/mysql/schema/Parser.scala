package com.neighborhood.aka.laplace.estuary.mysql.schema

import com.neighborhood.aka.laplace.estuary.bean.exception.schema.InvalidDdlException
import com.neighborhood.aka.laplace.estuary.core.util.JavaCommonUtil
import com.neighborhood.aka.laplace.estuary.mysql.schema.defs.ddl._

import scala.collection.JavaConverters._

/**
  * Created by john_liu on 2019/1/28.
  * 处理Ddl Sql 到 SchemaChange
  * 将SchemaChange 转换成Ddl Sql
  *
  * @note 传入的DDL只能是单条语句 or @throws InvalidDdlException
  * @author neighborhood.aka.laplace
  */
object Parser {

  /**
    * 解析sql 并进行库表名替换
    *
    * @param ddlSql
    * @param tableMappingRule
    * @return
    */
  def parseAndReplace(ddlSql: String, defaultSchemaName: String, tableMappingRule: SdaSchemaMappingRule): String = {

    parseAndReplaceInternal(ddlSql, defaultSchemaName, tableMappingRule).toDdlSql
  }


  private implicit class SchemaChangeToDdlSqlSyntax(schemaChange: SchemaChange) {
    def toDdlSql: String = SchemaChangeToDdlSql(schemaChange)
  }

  def SchemaChangeToDdlSql(schemaChange: SchemaChange): String = ???

  private def handleAlter(tableAlter: TableAlter): String = {
    val originName = s"${tableAlter.database}.${tableAlter.table}"
    val newName = s"${tableAlter.newDatabase}.${tableAlter.newTableName}"
    if (originName == newName) {
      //目前只支持单条
      tableAlter.columnMods.get(0) match {
        case add: AddColumnMod =>
          s"ALTER TABLE $newName ADD ${add.definition.getName} ${add.definition.getType} ${add.definition}";
        case remove: RemoveColumnMod => s"ALTER TABLE $newName DROP ${remove.name}"
        case change:ChangeColumnMod =>s"ALTER TABLE "
      }

    }
    else {
      s"RENAME $originName TO $newName;"
    }
  }

  private def handleCreate(tableCreate: TableCreate): String = {

  }

  private def handleDrop(tableDrop: TableDrop): String = ???

  /**
    * 解析Ddl sql
    *
    * @param ddlSql     待解析的ddl sql
    * @param schemaName 库名称
    * @return SchemaChange
    */
  private def parse(ddlSql: String, schemaName: String): List[SchemaChange] = {
    SchemaChange.parse(schemaName, ddlSql).asScala.toList
  }

  /**
    * 解析并替换库表名
    *
    * @param ddlSql            ddlSql
    * @param defaultSchemaName 默认库名称
    * @param tableMappingRule
    * @return
    */
  private def parseAndReplaceInternal(ddlSql: String, defaultSchemaName: String, tableMappingRule: SdaSchemaMappingRule): SchemaChange = {
    val re = parse(ddlSql, defaultSchemaName) //这个实现涉及了对象内部变量的改变
    if (re.size > 0) throw new InvalidDdlException("only single ddl is supported")
    re.head match {
      case alter: TableAlter => {
        if (JavaCommonUtil.isEmpty(alter.database)) alter.database = alter.newDatabase
        if (JavaCommonUtil.isEmpty(alter.table)) alter.table = alter.newTableName
        if (JavaCommonUtil.isEmpty(alter.newDatabase)) alter.newDatabase = alter.database
        if (JavaCommonUtil.isEmpty(alter.newTableName)) alter.newTableName = alter.table
        (alter.database, alter.table) = tableMappingRule.getMappingName(alter.database, alter.table)
        (alter.newDatabase, alter.newTableName) = tableMappingRule.getMappingName(alter.newDatabase, alter.newTableName)
      }
      case create: TableCreate => {
        if (!JavaCommonUtil.isEmpty(create.likeDB) && !JavaCommonUtil.isEmpty(create.likeTable)) (create.likeDB, create.likeTable) = tableMappingRule.getMappingName(create.likeDB, create.likeTable)
        (create.database, create.table) = tableMappingRule.getMappingName(create.database, create.table)
      }
      case drop: TableDrop => {
        (drop.database, drop.table) = tableMappingRule.getMappingName(drop.database, drop.table)
      }
    }

    re.head
  }


}
