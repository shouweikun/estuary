package com.neighborhood.aka.laplace.estuary.mysql.schema

import com.neighborhood.aka.laplace.estuary.mysql.schema.defs.ddl.{SchemaChange, TableAlter, TableCreate, TableDrop}
import scala.collection.JavaConverters._

/**
  * Created by john_liu on 2019/1/28.
  */
object Parser {

  def parse(ddlSql: String, schemaName: String): List[SchemaChange] = {
    SchemaChange.parse(schemaName, ddlSql).asScala.toList
  }

  def parseAndReplace(ddlSql: String, targetDbName: String, targetTableName: String): List[SchemaChange] = {
    val re = parse(ddlSql, targetDbName) //这个实现涉及了对象内部变量的改变
    re.map {
      x =>
        x match {
          case alter: TableAlter => alter.newDatabase = targetDbName; alter.newTableName = targetTableName;
          case create: TableCreate => create.database = targetDbName; create.table = targetTableName
          case drop: TableDrop => drop.database = targetDbName; drop.table = targetTableName
        }
    }
    re
  }

}
