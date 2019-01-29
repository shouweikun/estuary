package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.mappings

import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.MysqlRowDataInfo
import com.neighborhood.aka.laplace.estuary.mysql.schema.SdaSchemaMappingRule
import com.neighborhood.aka.laplace.estuary.mysql.schema.tablemeta.MysqlTableSchemaHolder
import com.puhui.aes.AesEncryptionUtil
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
  * @param encryptField        需要加密的表字段
  */
final class CanalEntry2RowDataInfoMappingFormat4Sda(
                                                     override val partitionStrategy: PartitionStrategy,
                                                     override val syncTaskId: String,
                                                     override val syncStartTime: Long,
                                                     override val schemaComponentIsOn: Boolean,
                                                     override val isCheckSchema: Boolean,
                                                     override val config: Config,
                                                     override val schemaHolder: Option[MysqlTableSchemaHolder] = None,
                                                     val tableMappingRule: SdaSchemaMappingRule,
                                                     val encryptField: Map[String, Set[String]]
                                                   ) extends CanalEntry2RowDataInfoMappingFormat {

  override protected lazy val logger = LoggerFactory.getLogger(classOf[CanalEntry2RowDataInfoMappingFormat4Sda])

  override def transform(x: lifecycle.EntryKeyClassifier): MysqlRowDataInfo = {
    val entry = x.entry
    val header = entry.getHeader
    val (dbName, tableName) = tableMappingRule.getMappingName(header.getSchemaName, header.getTableName)
    val dmlType = header.getEventType
    val columnList = x.columnList
    checkAndGetMysqlRowDataInfo(dbName, tableName, dmlType, columnList, entry)
  }

  /**
    * 加密函数
    */
  private final val aesEncrypt: PartialFunction[OperationField, OperationField] = new PartialFunction[OperationField, OperationField] {
    override def isDefinedAt(x: OperationField): Boolean = {
      val fullName = s"${x.entry.getHeader.getSchemaName}.${x.entry.getHeader.getTableName}"
      !x.value.startsWith("xy") && encryptField.contains(fullName) && encryptField(fullName).contains(x.column.getName)
    }

    override def apply(v1: OperationField): OperationField = v1.copy(value = AesEncryptionUtil.encrypt(v1.value.replaceAll("\u0000", "")))
  }

  override protected val valueMappingFunctions: List[PartialFunction[OperationField, OperationField]] = List(aesEncrypt, getSqlValueBySqlType)
}
