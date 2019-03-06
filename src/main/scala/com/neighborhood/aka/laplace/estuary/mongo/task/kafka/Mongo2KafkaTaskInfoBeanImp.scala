package com.neighborhood.aka.laplace.estuary.mongo.task.kafka

import java.util.Date

import com.neighborhood.aka.laplace.estuary.bean.identity.{BaseExtractBean, DataSyncType}

/**
  * Created by john_liu on 2019/2/28.
  */
final case class Mongo2KafkaTaskInfoBeanImp(syncTaskId: String)(val isCosting: Boolean = true) extends BaseExtractBean {

  /**
    * 数据同步形式
    */
  override def dataSyncType: String = DataSyncType.NORMAL.toString

  override protected def createTime: Date = ???

  override protected def lastChange: Date = ???
}
