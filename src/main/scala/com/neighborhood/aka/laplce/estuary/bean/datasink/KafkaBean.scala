package com.neighborhood.aka.laplce.estuary.bean.datasink

import java.util
import java.util.Properties

import com.neighborhood.aka.laplce.estuary.bean.datasink.DataSinkType.DataSinkType
import com.neighborhood.aka.laplce.estuary.bean.key.{JsonKeyPartitioner, JsonKeySerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created by john_liu on 2018/2/7.
  */
trait KafkaBean extends DataSinkBean {
  override var dataSinkType: DataSinkType = DataSinkType.KAFKA
  /**
    * broker地址
    */
  var bootstrapServers: String = ""
  /**
    * 最大阻塞时间
    */
  var maxBlockMs: String = "2000"
  var ack: String = "1"
  var lingerMs: String = "0"
  var kafkaRetries: String = "3"
  /**
    * 分区类
    */
  var partitionerClass: String = classOf[JsonKeyPartitioner].getName
  /**
    * topic
    */
  var topic: String = "test"
  /**
    * 最大接收数据
    */
  var maxRequestSize = "20971520"
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
  var keySerializer: String = classOf[JsonKeySerializer].getName
  /**
    * value Serializer类
    */
  var valueSerializer: String = classOf[StringSerializer].getName
  /**
    * 压缩格式
    */
  var compressionType: String = "snappy"

}

object KafkaBean {
  def buildConfig(kafkaBean: KafkaBean): util.HashMap[String, String] = {
    val config: util.HashMap[String, String] = new util.HashMap[String, String]()
    config.put("ack", kafkaBean.ack)
    config.put("lingerMs", kafkaBean.lingerMs)
    config.put("retries", kafkaBean.kafkaRetries)
    config.put("bootstrap.servers", kafkaBean.bootstrapServers)
    config.put("max.block.ms", kafkaBean.maxBlockMs)
    config.put("max.request.size", kafkaBean.maxRequestSize)
    config.put("fetch.max.bytes", kafkaBean.fetchMaxByte)
    config.put("request.timeout.ms", kafkaBean.requestTimeoutMs)
    config.put("message.send.max.retries", kafkaBean.messageSendMaxRetries)
    config.put("key.serializer", kafkaBean.keySerializer)
    config.put("value.serializer", kafkaBean.valueSerializer)
    config.put("partitioner.class", kafkaBean.partitionerClass)
    config.put("compression.type", kafkaBean.compressionType)
    config
  }
}
