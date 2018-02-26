package com.neighborhood.aka.laplce.estuary.bean.datasink

import java.util
import java.util.Properties

import com.neighborhood.aka.laplce.estuary.bean.datasink.DataSinkType.DataSinkType
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}

/**
  * Created by john_liu on 2018/2/7.
  */
trait KafkaBean extends DataSinkBean {
  override var dataSinkType: DataSinkType = DataSinkType.KAFKA
  /**
    * configJson
    * @example
    *"mqParams": {
      "bootstrap.servers": "kafka集群地址:端口",
      "max.block.ms": 3000,
      "max.request.size": 10485760,
      "request.timeout.ms": 8000,
      "acks": "1",
      "linger.ms": 0,
      "retries": 10
    }
    */
  var configMapFromJson:util.HashMap[String,String] = _
  /**
    * 分区类
    */
  var partitionerClass: String = "com.kafka.myparitioner.CidPartitioner"
  /**
    * 分区类
    */
  var requiredAcks: String = "1"
  /**
    * topic
    */
  var topic: String = "test"
  /**
    * 发送数据的超时阈值 单位秒
    */
  var sendTimeout: Long = 3
  /**
    * 最大接收数据
    */
  var maxResquestSize = "20971520"
  /**
    * 最大获取数据
    */
  var fetchMaxByte = "20971520"
  /**
    * request延迟
    */
  var requestTimeoutMs = "30000"
  /**
    * request重试次数
    */
  var messageSendMaxRetries = "3"
  /**
    * key Serializer类
    */
  var keySerializer: String = _
  /**
    * value Serializer类
    */
  var valueSerializer: String = _
  /**
    * 压缩格式
    */
  var compressionType: String = "snappy"

}
object KafkaBean {
  def buildConfig(kafkaBean: KafkaBean): util.HashMap[String, String] = {
    val config: util.HashMap[String, String] = new util.HashMap[String, String]()
    config.put("max.request.size", kafkaBean.maxResquestSize)
    config.put("fetch.max.bytes", kafkaBean.fetchMaxByte)
    config.put("request.timeout.ms", kafkaBean.requestTimeoutMs)
    config.put("message.send.max.retries",kafkaBean. messageSendMaxRetries)
    config.put("key.serializer",kafkaBean.keySerializer)
    config.put("value.serializer",kafkaBean.valueSerializer)
    config.put("partitioner.class",kafkaBean.partitionerClass)
    config.put("compression.type",kafkaBean.compressionType)
    config.putAll(kafkaBean.configMapFromJson)
    config
  }
}
