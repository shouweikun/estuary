package com.neighborhood.aka.laplace.estuary.mysql.sink

import com.neighborhood.aka.laplace.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc

/**
  * Created by john_liu on 2019/2/19.
  *
  * @author neighborhood.aka.laplace
  */
final class BinlogKeyKafkaSinkFunc(kafkaBean: BinlogKeyKafkaBeanImp) extends KafkaSinkFunc[BinlogKey, String](kafkaBean) {

  private val specificTopics: Map[String, String] = kafkaBean.specificTopics
  private val ddlTopic = kafkaBean.ddlTopic
  private val topic = kafkaBean.topic

  /**
    * 获取topic
    * 当有自己特殊要求时，请override这个方法
    *
    * @param key 关键字
    * @return match topic
    */
  override protected def getTopic(key: String): String = specificTopics.get(key).getOrElse {
    if (key.toLowerCase == "ddl") ddlTopic
    else topic
  }

}
