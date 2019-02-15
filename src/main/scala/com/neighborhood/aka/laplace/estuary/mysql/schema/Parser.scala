package com.neighborhood.aka.laplace.estuary.mysql.schema

import com.neighborhood.aka.laplace.estuary.bean.exception.schema.InvalidDdlException
import com.neighborhood.aka.laplace.estuary.core.util.JavaCommonUtil
import com.neighborhood.aka.laplace.estuary.mysql.schema.defs.columndef.{BigIntColumnDef, ColumnDef, IntColumnDef}
import com.neighborhood.aka.laplace.estuary.mysql.schema.defs.ddl._
import org.slf4j.LoggerFactory

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
  private lazy val logger = LoggerFactory.getLogger(Parser.getClass)

  /**
    * 解析Ddl sql
    *
    * @param ddlSql     待解析的ddl sql
    * @param schemaName 库名称
    * @return SchemaChange
    */
  def parse(ddlSql: String, schemaName: String): List[SchemaChange] = {
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
  def parseAndReplace(ddlSql: String, defaultSchemaName: String, tableMappingRule: SdaSchemaMappingRule): SchemaChange = {
    logger.info(s"start parse and replace ddl:$ddlSql,defaultSchemaName:$defaultSchemaName")
    val re = parse(ddlSql, defaultSchemaName) //这个实现涉及了对象内部变量的改变
    if (re.size > 1) throw new InvalidDdlException("only single ddl is supported")
    re.head match {
      case alter: TableAlter => {
        if (JavaCommonUtil.isEmpty(alter.database)) alter.database = alter.newDatabase
        if (JavaCommonUtil.isEmpty(alter.table)) alter.table = alter.newTableName
        if (JavaCommonUtil.isEmpty(alter.newDatabase)) alter.newDatabase = alter.database
        if (JavaCommonUtil.isEmpty(alter.newTableName)) alter.newTableName = alter.table
        val (database, table) = tableMappingRule.getMappingName(alter.database, alter.table)
        alter.database = database
        alter.table = table
        val (newDatabase, newTableName) = tableMappingRule.getMappingName(alter.newDatabase, alter.newTableName)
        alter.newDatabase = newDatabase
        alter.newTableName = newTableName
      }
      case create: TableCreate => {
        if (!JavaCommonUtil.isEmpty(create.likeDB) && !JavaCommonUtil.isEmpty(create.likeTable)) {
          val (likeDB, likeTable) = tableMappingRule.getMappingName(create.likeDB, create.likeTable)
          create.likeDB = likeDB
          create.likeTable = likeTable
        }
        val (database, table) = tableMappingRule.getMappingName(create.database, create.table)
        create.database = database
        create.table = table
      }
      case drop: TableDrop => {
        val (database, table) = tableMappingRule.getMappingName(drop.database, drop.table)
        drop.database = database
        drop.table = table
      }
    }

    re.head
  }

  /**
    * 提供隐式转换
    *
    * @param schemaChange 生成好的SchemaChange
    */
  implicit class SchemaChangeToDdlSqlSyntax(schemaChange: SchemaChange) {
    def toDdlSql: String = schemaChangeToDdlSql(schemaChange)
  }

  /**
    * 从SchemaChange转换成DdlSql
    *
    * @param schemaChange schemaChange
    * @return ddl sql String
    */
  def schemaChangeToDdlSql(schemaChange: SchemaChange): String = {
    schemaChange match {
      case tableAlter: TableAlter => handleAlter(tableAlter)
      case tableCreate: TableCreate => handleCreate(tableCreate)
      case tableDrop: TableDrop => handleDrop(tableDrop)
      case _ => throw new UnsupportedOperationException(s"do not support $schemaChange for now")
    }
  }

  /**
    * 从Alter/Rename语句转换成ddl sql
    *
    * @param tableAlter tableAlter
    * @return tableAlter
    */
  private def handleAlter(tableAlter: TableAlter): String = {
    val originName = s"${tableAlter.database}.${tableAlter.table}"
    val newName = s"${tableAlter.newDatabase}.${tableAlter.newTableName}"
    if (originName == newName) {
      //目前只支持单条
      tableAlter.columnMods.get(0) match {
        case add: AddColumnMod =>
          s"ALTER TABLE $newName ADD COLUMN ${add.definition.getName} ${add.definition.getFullType} ${getSigned(add.definition)} ${Option(add.definition.getDefaultValue).map(x => s"DEFAULT $x").getOrElse("")}"
        case remove: RemoveColumnMod => s"ALTER TABLE $newName DROP COLUMN ${remove.name}"
        case change: ChangeColumnMod => s"ALTER TABLE $newName CHANGE COLUMN ${Option(change.name).getOrElse("")} ${change.definition.getName} ${change.definition.getFullType} ${getSigned(change.definition)} ${Option(change.definition.getDefaultValue).map(x => s"DEFAULT $x").getOrElse("")}"
      }

    }
    else {
      s"RENAME $originName TO $newName;"
    }
  }

  /**
    * 创建表
    *
    * @param tableCreate 创建表语句-> ddl sql
    * @return ddl sql string
    */
  private def handleCreate(tableCreate: TableCreate): String = {
    lazy val pks = if (tableCreate.pks != null && !tableCreate.pks.isEmpty) tableCreate.pks.asScala.mkString(",") else ""
    lazy val pkGrammar = if (pks.nonEmpty)s""" , PRIMARY KEY ( $pks )""" else ""
    lazy val fieldGrammar = tableCreate.columns.asScala.map(col => s"${col.getName} ${col.getFullType} ${getSigned(col)} ${Option(col.getDefaultValue).map(x => s"DEFAULT $x").getOrElse("")}").mkString(",")
    s"""CREATE TABLE IF NOT EXISTS ${tableCreate.database}.${tableCreate.table}
       (
       $fieldGrammar
       ${pkGrammar}
       )ENGINE=InnoDB DEFAULT CHARSET=utf8
     """.stripMargin

  }

  /**
    * 处理删除语句 ddl -> ddl sql
    *
    * @param tableDrop
    * @return
    */
  private def handleDrop(tableDrop: TableDrop): String = {
    s"DROP TABLE IF EXISTS ${tableDrop.database}.${tableDrop.table}"
  }

  /**
    * 是否需要加unsigned
    *
    * @param columnDef
    * @return
    */
  private def getSigned(columnDef: ColumnDef): String = columnDef match {
    case c: IntColumnDef => if (!c.isSigned) "unsigned" else ""
    case c: BigIntColumnDef => if (!c.isSigned) "unsigned" else ""
    case _ => ""
  }


}
