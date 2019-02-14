package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.mappings

import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.MysqlRowDataInfo
import com.neighborhood.aka.laplace.estuary.mysql.schema.tablemeta.MysqlTableSchemaHolder
import com.typesafe.config.Config

/**
  * Created by john_liu on 2019/1/13.
  */
final class DefaultCanalEntry2RowDataInfoMappingFormat(
                                                        override val partitionStrategy: PartitionStrategy,
                                                        override val syncTaskId: String,
                                                        override val syncStartTime: Long,
                                                        override val schemaComponentIsOn: Boolean,
                                                        override val config: Config,
                                                        override val isCheckSchema: Boolean = true,
                                                        override val schemaHolder: Option[MysqlTableSchemaHolder]
                                                      ) extends CanalEntry2RowDataInfoMappingFormat {

  override def transform(x: lifecycle.EntryKeyClassifier): MysqlRowDataInfo = {
    val entry = x.entry
    val header = entry.getHeader
    val tableName = header.getTableName
    val dbName = header.getSchemaName
    val dmlType = header.getEventType
    val columnList = x.columnList
    checkAndGetMysqlRowDataInfo(dbName, tableName, dmlType, columnList, entry)
  }


}