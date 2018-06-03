package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import java.util.concurrent.ConcurrentHashMap

/**
  * Created by john_liu on 2018/6/3.
  */
class TableSchemaVersionCache {
  /**
    * key:DbName
    * value:Ca
    */
  val tableSchemaVersionMap: ConcurrentHashMap[String, Int] = new  ConcurrentHashMap[String, Int]()
}
