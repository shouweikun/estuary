package com.neighborhood.aka.laplace.estuary.mysql.schema

/**
  * Created by john_liu on 2019/1/25.
  *
  * sda tableMappingRule
  *
  * @author neighborhood.aka.laplace
  */
final class SdaSchemaMappingRule(
                                  private val tableMappingRule: Map[String, String]
                                ) {
  private lazy val databaseMappingRule: Map[String, String] = tableMappingRule.map {
    case (k, v) => (k.split('.')(0) -> v.split('.')(0))
  }

  /**
    * 获取mapping name
    *
    * warning:如果通过db.tb获取不到的话。返回的mapping是`(sdaDb->tb)`
    *
    * @param dbName    数据库名称
    * @param tableName 表名称
    * @return (db,tb)
    */
  def getMappingName(dbName: String, tableName: String): (String, String) = {
    tableMappingRule
      .get(s"$dbName.$tableName")
      .map { kv => (kv.split('.')(0), kv.split('.')(1)) }
      .getOrElse((getDatabaseMappingName(dbName).get, tableName)) //找不到情况是使用(sdaDb->tb) 如果找不到我们期望扔出异常
  }

  /**
    * 匹配原库->sda库对应明后才能
    *
    * @param dbName 数据库名称
    * @return sda库对应名称
    */
  def getDatabaseMappingName(dbName: String): Option[String] = databaseMappingRule.get(dbName)
}
