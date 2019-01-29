package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.mappings

import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.MysqlRowDataInfo
import com.neighborhood.aka.laplace.estuary.mysql.schema.SdaSchemaMappingRule
import com.neighborhood.aka.laplace.estuary.mysql.schema.tablemeta.MysqlTableSchemaHolder
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2019/1/13.
  *
  * @author neighborhood.aka.laplace
  * @param partitionStrategy   分区策略
  * @param syncTaskId          同步任务Id
  * @param syncStartTime       同步任务开始时间
  * @param schemaComponentIsOn 是否开启schema管理
  * @param config              typesafe.config
  * @param tableMappingRule    表名称映射规则
  * @param schemaHolder        schema校验
  */
final class CanalEntry2RowDataInfoMappingFormat4Sda(
                                                     override val partitionStrategy: PartitionStrategy,
                                                     override val syncTaskId: String,
                                                     override val syncStartTime: Long,
                                                     override val schemaComponentIsOn: Boolean,
                                                     override val isCheckSchema: Boolean,
                                                     override val config: Config,
                                                     override val schemaHolder: Option[MysqlTableSchemaHolder] = None,
                                                     val tableMappingRule: SdaSchemaMappingRule

                                                   ) extends CanalEntry2RowDataInfoMappingFormat {

  override protected lazy val logger = LoggerFactory.getLogger(classOf[CanalEntry2RowDataInfoMappingFormat4Sda])

  override def transform(x: lifecycle.EntryKeyClassifier): MysqlRowDataInfo = {
    val entry = x.entry
    val header = entry.getHeader
    val (dbName, tableName) = tableMappingRule.getMappingName(header.getSchemaName, header.getTableName)
    val dmlType = header.getEventType
    val rowData = x.rowData
    checkAndGetMysqlRowDataInfo(dbName, tableName, dmlType, rowData,entry)
  }


}
