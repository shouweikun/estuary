package com.neighborhood.aka.laplace.estuary.mongo.task.hbase

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.OplogClassifier
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.mappingFormat.Oplog2KafkaMessageMappingFormat
import com.neighborhood.aka.laplace.estuary.mongo.sink.hbase.{HBaseBeanImp, HBaseSinkManagerImp}
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoOffset, MongoSourceBeanImp, MongoSourceManagerImp}
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by john_liu on 2019/2/27.
  *
  * @author neighborhood.aka.laplace
  */
final class Mongo2HBaseTaskInfoManager(
                                        private val allTaskInfoBean: Mongo2HBaseAllTaskInfoBean,
                                        _config: Config
                                      ) extends HBaseSinkManagerImp with MongoSourceManagerImp with TaskManager {
  override protected lazy val logger: Logger = LoggerFactory.getLogger(classOf[Mongo2HBaseTaskInfoManager])
  /**
    * 数据汇bean
    */
  override val sinkBean: HBaseBeanImp = allTaskInfoBean.sinkBean

  /**
    * 数据源bean
    */
  override lazy val sourceBean: MongoSourceBeanImp = allTaskInfoBean.sourceBean

  /**
    * 任务信息bean
    */
  override lazy val taskInfo: Mongo2HBaseTaskInfoBeanImp = allTaskInfoBean.taskRunningInfoBean

  /**
    * batch转换模块
    */
  override lazy val batchMappingFormat: Option[MappingFormat[OplogClassifier, KafkaMessage]] = Option(buildMappingFormat)

  override val offsetZookeeperServer: String =taskInfo.offsetZookeeperServer

  override val startMongoOffset: Option[MongoOffset] = Option(taskInfo.mongoOffset)

  /**
    * 事件溯源的事件收集器
    */
  override def eventCollector: Option[ActorRef] = None //todo



  /**
    * 传入的配置
    *
    * @return
    */
  override lazy val config: Config = _config

  /**
    * 是否计数，默认不计数
    */
  override val isCounting: Boolean = taskInfo.isCounting

  /**
    * 是否计算每条数据的时间，默认不计时
    */
  override val isCosting: Boolean = taskInfo.isCosting

  /**
    * 是否保留最新binlog位置
    */
  override val isProfiling: Boolean = taskInfo.isProfiling

  /**
    * 是否打开功率调节器
    */
  override val isPowerAdapted: Boolean = taskInfo.isPowerAdapted

  /**
    * 是否同步写
    */
  override def isSync: Boolean = true //todo

  /**
    * 是否是补录任务
    */
  override def isDataRemedy: Boolean = true //todo

  /**
    * 任务类型
    * 由三部分组成
    * DataSourceType-DataSyncType-DataSinkType
    */
  override val taskType: String = s"${sourceBean.dataSourceType}-${taskInfo.dataSyncType}-${sinkBean.dataSinkType}"

  /**
    * 分区模式
    *
    * @return
    */
  override val partitionStrategy: PartitionStrategy = taskInfo.partitionStrategy

  /**
    * 是否阻塞式拉取
    *@todo
    * @return
    */
  override val isBlockingFetch: Boolean = true

  /**
    * 同步任务开始时间 用于fetch过滤无用字段
    *
    * @return
    */
  override val syncStartTime: Long = taskInfo.syncStartTime

  /**
    * 同步任务标识
    */
  override val syncTaskId: String = taskInfo.syncTaskId

  /**
    * 打包阈值
    */
  override val batchThreshold: Long = taskInfo.syncStartTime

  /**
    * batcher的数量
    */
  override val batcherNum: Int = taskInfo.batcherNum

  /**
    * sinker的数量
    */
  override val sinkerNum: Int = taskInfo.sinkerNum


  /**
    * 初始化/启动
    */
  override def start: Unit = {
    logger.info(s"task manager start,id:$syncTaskId")
    startSource
    startSink
  }

  private def buildMappingFormat: MappingFormat[OplogClassifier, KafkaMessage] = {
    logger.info(s"start to build build mapping formart,id:$syncTaskId")
    new Oplog2KafkaMessageMappingFormat(source, syncTaskId)
  }
}
