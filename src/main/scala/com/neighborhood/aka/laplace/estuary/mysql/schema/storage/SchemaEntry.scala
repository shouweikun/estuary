package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

/**
  * Created by john_liu on 2018/5/31.
  * @todo
  */
case class SchemaEntry(
                        val schemaName: String,
                        val tableName: String,
                        val version: Long,
                        val timestamp: Long,
                        val binlogFileName: String,
                        val binlogPosition: Long
                      ) {

  //每次新加字段都在这里更新一下
  lazy val fieldList: List[String] = List(schemaName, tableName, version, tableName, version, timestamp, binlogFileName, binlogPosition).map {
    x =>
      x match {
        case str:String => s"'$x'"
        case _ => x.toString
      }
  }

  /**
    *
    * @param targetSchemaName 给定的元数据信息数据库
    * @param targetTableName 表名
    * @return
    */
  def convertEntry2Sql(targetSchemaName: String, targetTableName: String): String = s"INSERT INTO $targetSchemaName.$targetTableName VALUES(${fieldList mkString (",")})"
}



