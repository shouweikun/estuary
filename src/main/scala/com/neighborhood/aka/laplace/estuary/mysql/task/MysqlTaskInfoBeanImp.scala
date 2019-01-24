package com.neighborhood.aka.laplace.estuary.mysql.task

import java.util.Date

import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplace.estuary.bean.identity.{BaseExtractBean, DataSyncType}
import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat

/**
  * Created by john_liu on 2019/1/13.
  *
  * mysql为数据源的任务信息Bean
  *
  * @author neighborhood.aka.laplace
  */
final case class MysqlTaskInfoBeanImp(
                                       val syncTaskId: String,
                                       val offsetZkServers: String
                                     )(
                                       val fetchDelay: Long = 0, //todo 还未增加配置选项中
                                       val syncStartTime: Long = 0l,
                                       val startPosition: Option[EntryPosition] = None,
                                       val mysqlDatabaseNameList: List[String] = Nil,
                                       val batchThreshold: Long = 1,
                                       val schemaComponentIsOn: Boolean = false,
                                       val isNeedExecuteDDL: Boolean = false,
                                       val isCounting: Boolean = true,
                                       val isCosting: Boolean = true,
                                       val isProfiling: Boolean = true,
                                       val isPowerAdapted: Boolean = true,
                                       val partitionStrategy: PartitionStrategy = PartitionStrategy.PRIMARY_KEY,
                                       val controllerNameToLoad: Map[String, String] = Map.empty,
                                       val sinkerNameToLoad: Map[String, String] = Map.empty,
                                       val fetcherNameToLoad: Map[String, String],
                                       val batcherNameToLoad: Map[String, String],
                                       val batcherNum: Int = 23,
                                       val batchMappingFormatName: Option[String] = None,
                                       val isCheckSinkSchema:Boolean = false
                                     ) extends BaseExtractBean {

  /**
    * 数据同步形式
    */
  override def dataSyncType: String = DataSyncType.NORMAL.toString

  override protected def createTime: Date = ???

  override protected def lastChange: Date = ???
}
