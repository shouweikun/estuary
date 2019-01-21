package com.neighborhood.aka.laplace.estuary.bean.datasink


import com.neighborhood.aka.laplace.estuary.bean.key._
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created by john_liu on 2018/2/7.
  *
  * @todo 未完成
  */
trait KafkaBean extends DataSinkBean[MysqlSinkFunc] {
  //  override var dataSinkType: DataSinkType = DataSinkType.KAFKA
  override val dataSinkType: String = SinkDataType.KAFKA.toString

  var partitionStrategy: PartitionStrategy = PartitionStrategy.PRIMARY_KEY
  /**
    * broker地址
    */
  var bootstrapServers: String = ""
  /**
    * 最大阻塞时间
    */
  var maxBlockMs: String = "20000"
  var ack: String = "all"
  var lingerMs: String = "30"
  var kafkaRetries: String = "3"
  /**
    * 分区类
    */
  var partitionerClass: String = classOf[MultipleJsonKeyPartitioner].getName
  /**
    * defaultTopic
    */
  var topic: String = _
  /**
    * ddl专用Topic
    */
  var ddlTopic: String = _
  /**
    * 依照特定规则映射表和数据库找出topic
    */
  var specificTopics: Map[String, String] = Map.empty
  /**
    * 最大接收数据
    */
  var maxRequestSize = "20971520"
  /**
    * request延迟
    */
  var requestTimeoutMs = "35000"
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
  /**
    * 批大小
    */
  var batchSize: String = "10000"

  var retryBackoffMs: String = "300"
  /**
    * 是否是同步写
    */
  var isSync = false

  var maxInFlightRequestsPerConnection = "1"

}

object KafkaBean {
  def buildConfig(kafkaBean: KafkaBean): java.util.HashMap[String, String] = {
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
