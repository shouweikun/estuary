package com.neighborhood.aka.laplace.estuary.bean.datasink


import com.neighborhood.aka.laplace.estuary.bean.key._
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc

/**
  * Created by john_liu on 2018/2/7.
  *
  * @author neighborhood.aka.laplace
  * @tparam K 消息Key的类型
  * @tparam V 消息Value的类型
  */
trait KafkaBean[K, V] extends DataSinkBean[MysqlSinkFunc] {
  //  override var dataSinkType: DataSinkType = DataSinkType.KAFKA
  override val dataSinkType: String = SinkDataType.KAFKA.toString

  /**
    * 分区策略
    */
  def partitionStrategy: PartitionStrategy = PartitionStrategy.PRIMARY_KEY

  /**
    * broker地址
    */
  def bootstrapServers: String

  /**
    * 最大阻塞时间
    */
  def maxBlockMs: String = "20000"

  def ack: String = "all"

  def lingerMs: String = "30"

  def kafkaRetries: String = "3"

  /**
    * 分区类
    */
  def partitionerClass: String

  /**
    * defaultTopic
    */
  def topic: String

  /**
    * ddl专用Topic
    */
  def ddlTopic: String

  /**
    * 依照特定规则映射表和数据库找出topic
    */
  def specificTopics: Map[String, String] = Map.empty

  /**
    * 最大接收数据
    */
  def maxRequestSize = "20971520"

  /**
    * request延迟
    */
  def requestTimeoutMs = "35000"

  /**
    * key Serializer类
    */
  def keySerializer: String

  /**
    * value Serializer类
    */
  def valueSerializer: String

  /**
    * 压缩格式
    */
  def compressionType: String = "snappy"

  /**
    * 批大小
    */
  def batchSize: String = "10000"

  def retryBackoffMs: String = "300"

  /**
    * 是否是同步写
    */
  def isSync = false

  /**
    * 每次执行的批次
    */
  def maxInFlightRequestsPerConnection = "1"

}

object KafkaBean {
  def buildConfig(kafkaBean: KafkaBean[_,_]): java.util.HashMap[String, String] = {
    val config: java.util.HashMap[String, String] = new java.util.HashMap[String, String]()
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
    config.put("batch.size", kafkaBean.batchSize)
    config.put("retry.backoff.ms", kafkaBean.retryBackoffMs)
    config.put("max.in.flight.requests.per.connection", kafkaBean.maxInFlightRequestsPerConnection)
    config
  }
}
