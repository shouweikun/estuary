package com.neighborhood.aka.laplce.estuary.bean.datasink

import java.util.Properties

import com.neighborhood.aka.laplce.estuary.bean.datasink.DataSinkType.DataSinkType
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}

/**
  * Created by john_liu on 2018/2/7.
  */
trait KafkaBean extends DataSinkBean {
  override var dataSinkType: DataSinkType = DataSinkType.KAFKA

  var brokerList: String = ""
  var serializerClass: String = ""
  var partitionerClass: String = ""
  var requiredAcks: String = ""

  var topic:String = ""
  /**
    * 发送数据的超时阈值 单位秒
    */
  var sendTimeout:Long = 3

}
