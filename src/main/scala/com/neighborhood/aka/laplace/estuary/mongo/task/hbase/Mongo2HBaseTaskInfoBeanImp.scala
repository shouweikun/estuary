package com.neighborhood.aka.laplace.estuary.mongo.task.hbase

import java.util.Date

import com.neighborhood.aka.laplace.estuary.bean.identity.{BaseExtractBean, DataSyncType}
import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset

/**
  * Created by john_liu on 2019/2/28.
  */
final case class Mongo2HBaseTaskInfoBeanImp(
                                             val syncTaskId: String,
                                             val offsetZookeeperServer: String

                                           )(
                                             val mongoOffset: MongoOffset = MongoOffset((System.currentTimeMillis() / 1000).toInt, 0),
                                             val isCosting: Boolean = true,
                                             val isCounting: Boolean = true,
                                             val isProfiling: Boolean = true,
                                             val isPowerAdapted: Boolean = true,
                                             val partitionStrategy: PartitionStrategy = PartitionStrategy.MOD,
                                             val syncStartTime: Long = System.currentTimeMillis(),
                                             val batchThreshold: Long = 1,
                                             val batcherNum: Int = 15,
                                             val sinkerNum: Int = 15,
                                             val logEnabled:Boolean = true,
                                             val fetcherNameToLoad:Map[String,String]
                                           ) extends BaseExtractBean {

  /**
    * 数据同步形式
    */
  override def dataSyncType: String = DataSyncType.NORMAL.toString

  override protected def createTime: Date = ???

  override protected def lastChange: Date = ???
}
