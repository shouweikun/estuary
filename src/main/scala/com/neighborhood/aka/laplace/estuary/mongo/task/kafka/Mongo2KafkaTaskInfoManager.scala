package com.neighborhood.aka.laplace.estuary.mongo.task.kafka

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.bean.identity.BaseExtractBean
import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.bean.resource.DataSourceBase
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat
import com.neighborhood.aka.laplace.estuary.mongo.sink.{OplogKeyKafkaBeanImp, OplogKeyKafkaSinkManagerImp}
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoSourceManagerImp}
import com.typesafe.config.Config

/**
  * Created by john_liu on 2019/2/27.
  */
final class Mongo2KafkaTaskInfoManager extends OplogKeyKafkaSinkManagerImp with MongoSourceManagerImp with TaskManager {
  /**
    * 数据汇bean
    */
  override def sinkBean: OplogKeyKafkaBeanImp = ???

  /**
    * batch转换模块
    */
  override def batchMappingFormat: Option[MappingFormat[_, _]] = ???

  /**
    * 事件溯源的事件收集器
    */
  override def eventCollector: Option[ActorRef] = ???

  /**
    * 任务信息bean
    */
  override def taskInfo: BaseExtractBean = ???

  /**
    * 传入的配置
    *
    * @return
    */
  override def config: Config = ???

  /**
    * 是否计数，默认不计数
    */
  override def isCounting: Boolean = ???

  /**
    * 是否计算每条数据的时间，默认不计时
    */
  override def isCosting: Boolean = ???

  /**
    * 是否保留最新binlog位置
    */
  override def isProfiling: Boolean = ???

  /**
    * 是否打开功率调节器
    */
  override def isPowerAdapted: Boolean = ???

  /**
    * 是否同步写
    */
  override def isSync: Boolean = ???

  /**
    * 是否是补录任务
    */
  override def isDataRemedy: Boolean = ???

  /**
    * 任务类型
    * 由三部分组成
    * DataSourceType-DataSyncType-DataSinkType
    */
  override def taskType: String = ???

  /**
    * 分区模式
    *
    * @return
    */
  override def partitionStrategy: PartitionStrategy = ???

  /**
    * 是否阻塞式拉取
    *
    * @return
    */
  override def isBlockingFetch: Boolean = ???

  /**
    * 同步任务开始时间 用于fetch过滤无用字段
    *
    * @return
    */
  override def syncStartTime: Long = ???

  /**
    * 同步任务标识
    */
  override def syncTaskId: String = ???

  /**
    * 打包阈值
    */
  override def batchThreshold: Long = ???

  /**
    * batcher的数量
    */
  override def batcherNum: Int = ???

  /**
    * sinker的数量
    */
  override def sinkerNum: Int = ???

  /**
    * 数据源bean
    */
  override def sourceBean: DataSourceBase[MongoConnection] = ???

  /**
    * 构建数据源
    *
    * @return source
    */
  override def buildSource: MongoConnection = ???
}
