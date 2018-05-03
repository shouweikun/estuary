package com.neighborhood.aka.laplace.estuary.bean.task

import com.neighborhood.aka.laplace.estuary.bean.credential.MongoCredentialBean
import com.neighborhood.aka.laplace.estuary.bean.datasink.KafkaBean
import com.neighborhood.aka.laplace.estuary.bean.identity.{BaseExtractBean, SyncDataType}
import com.neighborhood.aka.laplace.estuary.bean.resource.{MongoBean, MysqlBean}

/**
  * Created by john_liu on 2018/4/25.
  */
class Mongo2KafkaTaskInfoBean(
                               val mongoCredentials: List[MongoCredentialBean],
                               val hosts: List[String],
                               val port: Int
                             ) extends MongoBean with KafkaBean with BaseExtractBean {
  /**
    * 数据同步形式
    */
  override var dataSyncType: String = SyncDataType.NORMAL.toString

  val

}
