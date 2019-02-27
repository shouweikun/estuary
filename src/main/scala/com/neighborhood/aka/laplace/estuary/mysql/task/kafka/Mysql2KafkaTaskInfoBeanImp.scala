package com.neighborhood.aka.laplace.estuary.mysql.task.kafka

import java.util.Date

import com.neighborhood.aka.laplace.estuary.bean.identity.{BaseExtractBean, DataSyncType}

/**
  * Created by john_liu on 2019/2/27.
  */
final case class Mysql2KafkaTaskInfoBeanImp(syncTaskId: String) extends BaseExtractBean {

  /**
    * 数据同步形式
    */
  override def dataSyncType: String = DataSyncType.NORMAL.toString

  override protected def createTime: Date = ???

  override protected def lastChange: Date = ???
}
