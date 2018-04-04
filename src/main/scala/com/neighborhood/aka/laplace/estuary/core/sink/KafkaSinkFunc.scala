package com.neighborhood.aka.laplace.estuary.core.sink

import java.util.Properties
import java.util.concurrent.Future

import com.neighborhood.aka.laplace.estuary.bean.datasink.KafkaBean
import com.neighborhood.aka.laplace.estuary.bean.key.BaseDataJsonKey
import org.apache.kafka.clients.producer._

import scala.util.{Failure, Success, Try}

/**
  * Created by john_liu on 2018/2/7.
  */
class KafkaSinkFunc[V](kafkaBean: KafkaBean) extends SinkFunc {
  //  /**
  //    * 异步写入时的ExecutionContext
  //    * 值得注意的是，该ExecutionContext是Kafka公用的
  //    */
  //  implicit val ec = KafkaSinkFunc.ec
  /**
    * Kafka生产者
    */
  lazy val kafkaProducer = KafkaSinkFunc.buildKafkaProducer[V](kafkaBean)
  /**
    * defaultTopic
    */
  val topic = kafkaBean.topic
  /**
    * 库表名和topic映射map
    */
  val specificTopics = kafkaBean.specificTopics
  /**
    * ddl用topic
    * //todo
    */
  val ddlTopic = kafkaBean.ddlTopic

  /**
    * @param key   分区key
    * @param value 待写入的数据
    * @return Boolean 是否写入成功
    */
  def sink(key: BaseDataJsonKey, value: V)(topic: String): Boolean = {
    val record = buildRecord(key, value, topic)
    kafkaProducer.send(record).get()
    true
  }

  /**
    * @param key   分区key
    * @param value 待写入的数据
    * @return Future[RecordMetadata 是否写入成功
    */
  def ayncSink(key: BaseDataJsonKey, value: V)(topic: String)(callback: Callback): Future[RecordMetadata] = {
    val record = buildRecord(key, value, topic)
    kafkaProducer.send(record, callback)
  }

  def flush: Unit = {
    kafkaProducer.flush()
  }

  def findTopic(key: String = ""): String = {
    specificTopics
      .get(key) match {
      case None =>
        if (key == "DDL") ddlTopic else this.topic
      case Some(tpc) => tpc


    }

  }


  //  /**
  //    * @param source 待写入的数据
  //    * @return Future[Boolean] 是否写入成功
  //    */
  //   def asyncSink(source: V): Future[Boolean] = {
  //    Future {
  //      sink(source)
  //    }
  //  }

  /**
    * @param key   分区key
    * @param value 待写入的数据
    * @return ProducerRecord[String,V]
    */
  def buildRecord(key: BaseDataJsonKey, value: V, topic: String): ProducerRecord[BaseDataJsonKey, V] = {
    key.setMsgSyncEndTime(System.currentTimeMillis())
    new ProducerRecord[BaseDataJsonKey, V](topic, key, value)
  }
}

object KafkaSinkFunc {

  //  val ec = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(3))
  /**
    * @param kafkaBean
    * @return KafkaProducer
    *         根据kafkaBean的参数设置,初始化一个producer
    */
  def buildKafkaProducer[V](kafkaBean: KafkaBean): KafkaProducer[BaseDataJsonKey, V] = {
    val props = new Properties()
    props.putAll(KafkaBean.buildConfig(kafkaBean))
    new KafkaProducer[BaseDataJsonKey, V](props)
  }

}
