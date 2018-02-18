package com.neighborhood.aka.laplce.estuary.bean.datasink

import java.util.Properties

import com.neighborhood.aka.laplce.estuary.bean.datasink.DataSinkType.DataSinkType
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}

/**
  * Created by john_liu on 2018/2/7.
  */
trait KafkaBean extends DataSinkBean {
  override var dataSinkType: DataSinkType = DataSinkType.KAFKA

  var brokerList: String = "10.10.71.76:9092,10.10.71.14:9092,10.10.72.234:9092"
  var serializerClass: String = "kafka.serializer.StringEncoder"
  var partitionerClass: String = "com.kafka.myparitioner.CidPartitioner"
  var requiredAcks: String = "1"

  var topic:String = "test"
  /**
    * 发送数据的超时阈值 单位秒
    */
  var sendTimeout:Long = 3

}
