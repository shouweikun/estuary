package com.neighborhood.aka.laplace.estuary.mysql.schema.defs.ddl

/**
  * Created by john_liu on 2019/1/28.
  *
  * 将SchemaChange 转换成Ddl Sql
  *
  * @author neighborhood.aka.laplace
  */
object SchemaChangeConverter {

  implicit class SchemaChangeToDdlSqlSyntax(schemaChange: SchemaChange) {
    def toDdlSql: String = SchemaChangeToDdlSql(schemaChange)
  }

  def SchemaChangeToDdlSql(schemaChange: SchemaChange): String = ???

  private def handleAlter(tableAlter: TableAlter): String = ???

  private def handleCreate(tableCreate: TableCreate): String = ???

  private def handleDrop(tableDrop: TableDrop): String = ???
}
