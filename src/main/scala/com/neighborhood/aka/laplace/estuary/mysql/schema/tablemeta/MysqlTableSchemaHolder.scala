package com.neighborhood.aka.laplace.estuary.mysql.schema.tablemeta

/**
  * Created by john_liu on 2019/1/23.
  *
  * mysql tables的元数据信息
  *
  * @author neighborhood.aka.laplace
  */
final case class MysqlTableSchemaHolder(
                                         @volatile private var tableSchemas: Map[String, EstuaryMysqlTableMeta]
                                       ) {

  def getTableMetaByFullName(fullName: String): Option[EstuaryMysqlTableMeta] = tableSchemas.get(fullName)

  def addNewTableMeta(fullName: String, m: EstuaryMysqlTableMeta): Unit = tableSchemas = tableSchemas.+(fullName -> m)
}
