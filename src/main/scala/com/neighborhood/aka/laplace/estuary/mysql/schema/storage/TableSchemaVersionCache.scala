package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import java.util.concurrent.ConcurrentHashMap

import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.SchemaEntry.MysqlConSchemaEntry

/**
  * Created by john_liu on 2018/6/3.
  */
class TableSchemaVersionCache(val dbName: String) {
  /**
    * key:DbName
    * value:Ca
    */
  val tableSchemaVersionMap: ConcurrentHashMap[String, List[MysqlConSchemaEntry]] = new ConcurrentHashMap[String, List[MysqlConSchemaEntry]]()


  def upsertSchemas(schema: MysqlConSchemaEntry): Unit = {
    lazy val tableName = schema.tableId
    if (tableSchemaVersionMap.containsKey(tableName)) {
      val oldSchemas = tableSchemaVersionMap.get(tableName)
      val newSchemas = oldSchemas.:+(schema)
      tableSchemaVersionMap.put(tableName, newSchemas)
    } else {
      tableSchemaVersionMap.put(tableName,s)
    }
  }

  def getLatestVersion(tableName: String): Long = ???
}
