package com.neighborhood.aka.laplace.estuary.bean.datasink

import java.util

import com.neighborhood.aka.laplace.estuary.bean.datasink.DataSinkType.DataSinkType
import com.neighborhood.aka.laplace.estuary.bean.key.{JsonKeyPartitioner, JsonKeySerializer}
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
  var maxBlockMs: String = "5000"
  var ack: String = "1"
  var lingerMs: String = "0"
  var kafkaRetries: String = "3"
  /**
    * 分区类
    */
  var partitionerClass: String = classOf[JsonKeyPartitioner].getName
  /**
    * defaultTopic
    */
  var topic: String = _
  /**
    * 依照特定规则映射表和数据库找出topic
    */
  var specificTopics: Map[String,String] = Map.empty
  /**
    * 最大接收数据
    */
  var maxRequestSize = "20971520"
  /**
    * request延迟
    */
  var requestTimeoutMs = "30000"
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
    config.put("acks", kafkaBean.ack)
    config.put("linger.ms", kafkaBean.lingerMs)
    config.put("retries", kafkaBean.kafkaRetries)
    config.put("bootstrap.servers", kafkaBean.bootstrapServers)
    config.put("max.block.ms", kafkaBean.maxBlockMs)
    config.put("max.request.size", kafkaBean.maxRequestSize)
    config.put("request.timeout.ms", kafkaBean.requestTimeoutMs)
    config.put("key.serializer", kafkaBean.keySerializer)
    config.put("value.serializer", kafkaBean.valueSerializer)
    config.put("partitioner.class", kafkaBean.partitionerClass)
    config.put("compression.type", kafkaBean.compressionType)
    config
  }
}
