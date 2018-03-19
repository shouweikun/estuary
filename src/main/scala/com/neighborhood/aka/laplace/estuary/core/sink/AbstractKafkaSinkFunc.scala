package com.neighborhood.aka.laplace.estuary.core.sink

import java.util.concurrent.Future

import com.neighborhood.aka.laplace.estuary.bean.datasink.KafkaBean
import com.neighborhood.aka.laplace.estuary.bean.key.BaseDataJsonKey
import org.apache.kafka.clients.producer._

import scala.util.{Failure, Success, Try}

/**
  * Created by john_liu on 2018/2/7.
  */
class AbstractKafkaSinkFunc[K <: BaseDataJsonKey, V](kafkaBean: KafkaBean) extends SinkFunc {


  /**
    * @param key   分区key
    * @param value 待写入的数据
    * @return ProducerRecord[String,V]
    */
  def buildRecord(key: K, value: V)(topic: String): ProducerRecord[K, V] = {
    key.setMsgSyncEndTime(System.currentTimeMillis())
    new ProducerRecord[K, V](topic, key, value)
  }
}


