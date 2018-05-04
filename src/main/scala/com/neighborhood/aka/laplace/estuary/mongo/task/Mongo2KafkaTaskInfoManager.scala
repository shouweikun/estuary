package com.neighborhood.aka.laplace.estuary.mongo.task

import com.neighborhood.aka.laplace.estuary.core.sink.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{RecourceManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoConnection
import com.neighborhood.aka.laplace.estuary.mongo.utils.MongoOffsetHandler

/**
  * Created by john_liu on 2018/5/2.
  */
class Mongo2KafkaTaskInfoManager(
                                 val taskInfoBean: Mongo2KafkaTaskInfoBean
                                )
  extends TaskManager with RecourceManager[String, MongoConnection, KafkaSinkFunc[String]] {
  lazy val mongoOffsetHandler = buildMongoOffsetHandler
  lazy val mongoConnection = buildSource
  lazy val kafkaSink = buildSink

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
}
