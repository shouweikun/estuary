package com.neighborhood.aka.laplace.estuary.mongo.task

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.bean.datasink.DataSinkBean
import com.neighborhood.aka.laplace.estuary.bean.resource.DataSourceBase
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{RecourceManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoConnection
import com.neighborhood.aka.laplace.estuary.mongo.utils.MongoOffsetHandler

/**
  * Created by john_liu on 2018/5/2.
  */
class Mongo2KafkaTaskInfoManager(
                                  override val taskInfoBean: Mongo2KafkaTaskInfoBean
                                )
  extends TaskManager with RecourceManager[String, MongoConnection, KafkaSinkFunc[String]] {

  /**
    * 是否计数，默认不计数
    */
  override val isCounting: Boolean = true
  /**
    * 是否计算每条数据的时间，默认不计时
    */
  override val isCosting: Boolean = true
  /**
    * 是否保留最新binlog位置
    */
  override val isProfiling: Boolean = true
  /**
    * 是否打开功率调节器
    */
  override val isPowerAdapted: Boolean = true
  /**
    * sinker的数量
    */
  override val sinkerNum: Int = 0
  /**
    * 监听心跳用的语句
    */
  override val delectingCommand: String = ""
  /**
    * 监听重试次数标准值
    */
  override val listeningRetryTimeThreshold: Int = 3
  override val sinkBean: DataSinkBean = taskInfoBean
  override val sourceBean: DataSourceBase = taskInfoBean

  lazy val mongoOffsetHandler = buildMongoOffsetHandler
  lazy val mongoConnection = buildSource
  lazy val kafkaSink = buildSink
  /**
    * 功率控制器
    */
  var powerAdapter: Option[ActorRef] = None
  /**
    * 计数器
    */
  var processingCounter: Option[ActorRef] = None
  /**
    *
    */
  override val batcherNum: Int = taskInfoBean.batcherNum

  /**
    * 任务类型
    * 由三部分组成
    * DataSourceType-DataSyncType-DataSinkType
    */
  override def taskType: String = s"${taskInfoBean.dataSourceType}-${taskInfoBean.dataSyncType}-${taskInfoBean.dataSinkType}"


  override def buildSource: MongoConnection = {
    new MongoConnection(taskInfoBean)
  }

  override def buildSink: KafkaSinkFunc[String] = ???

  def buildMongoOffsetHandler: MongoOffsetHandler = {
    ???
  }

  /**
    * 同步任务标识
    */
  override val syncTaskId: String = taskInfoBean.syncTaskId
}
