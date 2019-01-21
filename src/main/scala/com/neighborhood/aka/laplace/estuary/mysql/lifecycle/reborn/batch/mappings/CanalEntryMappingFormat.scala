package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.mappings

import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.EntryKeyClassifier
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.MysqlSchemaHandler
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2019/1/10.
  */
trait CanalEntryMappingFormat[R] extends MappingFormat[EntryKeyClassifier, R] {

  protected val logger = LoggerFactory.getLogger(classOf[CanalEntryMappingFormat[R]])


  /**
    * 分区策略
    *
    * @return
    */
  def partitionStrategy: PartitionStrategy

  /**
    * 同步任务Id
    */
  def syncTaskId: String

  /**
    * 任务开始时间
    */
  def syncStartTime: Long

//  /**
//    * 对应的batcher编号
//    */
//  def num: Int

  /**
    * 元数据信息处理器
    */
  def mysqlSchemaHandler: MysqlSchemaHandler

  /**
    * 是否开启Schema管理
    *
    * @return
    */
  def schemaComponentIsOn: Boolean

  /**
    * config
    */
  def config: Config

}
