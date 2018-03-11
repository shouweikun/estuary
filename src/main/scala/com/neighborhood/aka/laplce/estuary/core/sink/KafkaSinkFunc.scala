package com.neighborhood.aka.laplce.estuary.core.sink

import java.util.Properties
import java.util.concurrent.Future

import com.neighborhood.aka.laplce.estuary.bean.datasink.KafkaBean
import com.neighborhood.aka.laplce.estuary.bean.key.BaseDataJsonKey
import org.apache.kafka.clients.producer._

import scala.util.{Failure, Success, Try}

/**
  * Created by john_liu on 2018/2/7.
  */
class KafkaSinkFunc[K <: BaseDataJsonKey, V](kafkaBean: KafkaBean) extends SinkFunc {
  //  /**
  //    * 异步写入时的ExecutionContext
  //    * 值得注意的是，该ExecutionContext是Kafka公用的
  //    */
  //  implicit val ec = KafkaSinkFunc.ec
  /**
    * Kafka生产者
    */
  lazy val kafkaProducer = KafkaSinkFunc.buildKafkaProducer[K, V](kafkaBean)
  /**
    * 待写入的topic
    */
  val topic = kafkaBean.topic


  /**
    * @param key   分区key
    * @param value 待写入的数据
    * @return Boolean 是否写入成功
    */
  def sink(key: K, value: V): Boolean = {
    val record = buildRecord(key, value)
    Try(kafkaProducer.send(record).get()) match {
      case Success(x) => {
        //todo log
        true
      }
      case Failure(e) => {
        //todo log
        e
        kafkaProducer.close()
        false
      }

    }

  }

  /**
    * @param key   分区key
    * @param value 待写入的数据
    * @return Future[RecordMetadata 是否写入成功
    */
  def ayncSink(key: K, value: V)(topic:String)(callback: Callback): Future[RecordMetadata] = {
    val record = buildRecord(key, value)
    kafkaProducer.send(record, callback)
  }

  def flush: Unit = {
    kafkaProducer.flush()
  }

  def findTopic(key:String = ""):String = {
    //todo
    this.topic
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
  def buildRecord(key: K, value: V): ProducerRecord[K, V] = {
    key.setMsgSyncEndTime(System.currentTimeMillis())
    new ProducerRecord[K, V](topic, key, value)
  }
}

object KafkaSinkFunc {

  //  val ec = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(3))
  /**
    * @param kafkaBean
    * @return KafkaProducer
    *         根据kafkaBean的参数设置,初始化一个producer
    */
  def buildKafkaProducer[K, V](kafkaBean: KafkaBean): KafkaProducer[K, V] = {
    val props = new Properties()
    props.putAll(KafkaBean.buildConfig(kafkaBean))
    new KafkaProducer[K, V](props)
  }

}
