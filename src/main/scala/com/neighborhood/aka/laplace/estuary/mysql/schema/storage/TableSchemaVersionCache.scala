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
  val tableSchemaVersionMap: ConcurrentHashMap[String, MysqlConSchemaEntry] = new ConcurrentHashMap[String, MysqlConSchemaEntry]()


  def upsertSchemas(schema: MysqlConSchemaEntry): Unit = {
    if (tableSchemaVersionMap.contains(schema.tableName)) {


    }

  }

  def getLatestVersion(tableName: String): Long = ???
}
