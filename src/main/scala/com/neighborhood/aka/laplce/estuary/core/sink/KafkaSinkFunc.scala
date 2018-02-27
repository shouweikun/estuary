package com.neighborhood.aka.laplce.estuary.core.sink

import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import com.neighborhood.aka.laplce.estuary.bean.datasink.KafkaBean
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * Created by john_liu on 2018/2/7.
  */
class KafkaSinkFunc[K, V](kafkaBean: KafkaBean) extends SinkFunc[V] {
  /**
    * 异步写入时的ExecutionContext
    * 值得注意的是，该ExecutionContext是Kafka公用的
    */
  implicit val ec = KafkaSinkFunc.ec
  /**
    * Kafka生产者
    */
  lazy val kafkaProducer = KafkaSinkFunc.buildKafkaProducer[K, V](kafkaBean)
  /**
    * 待写入的topic
    */
  val topic = kafkaBean.topic
  /**
    * 写入数据超时设置
    */
  val timeLimit = kafkaBean.sendTimeout

  /**
    * @param source 待写入的数据
    * @return Future[Boolean] 是否写入成功
    */
  override def sink(source: V): Boolean = {
    val record = buildRecord(source)
    Try(kafkaProducer.send(record).get()) match {
      case Success(x) => {
        //todo log
        true
      }
      case Failure(e) => {
        //todo log
        e
        false
      }
    }
  }

  /**
    * @param source 待写入的数据
    * @return Future[Boolean] 是否写入成功
    */
  override def asyncSink(source: V): Future[Boolean] = {
    Future {
      sink(source)
    }
  }

  /**
    * @param source 待写入的数据
    * @return ProducerRecord[String,V]
    */
  def buildRecord(source: V): ProducerRecord[K, V] = {
    new ProducerRecord[K, V](topic, source)
  }
}

object KafkaSinkFunc {

  val ec = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(3))
  /**
    * @param kafkaBean
    * @return KafkaProducer
    * 根据kafkaBean的参数设置,初始化一个producer
    */
  def buildKafkaProducer[K, V](kafkaBean: KafkaBean): KafkaProducer[K, V] = {
    val props = new Properties()
    props.putAll(KafkaBean.buildConfig(kafkaBean))
    new KafkaProducer[K, V](props)
  }

}
