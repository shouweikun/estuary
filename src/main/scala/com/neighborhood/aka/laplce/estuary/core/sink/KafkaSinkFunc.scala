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
class KafkaSinkFunc[String, V](kafkaBean: KafkaBean) extends SinkFunc[V] {
  implicit val ec = KafkaSinkFunc.ec
  lazy val kafkaProducer = KafkaSinkFunc.buildKafkaProducer[String, V](kafkaBean)
  val topic = kafkaBean.topic
  val timeLimit = kafkaBean.sendTimeout

  override def sink(source: V): Boolean = {
    val record = buildRecord(source)
    Try(kafkaProducer.send(record).get(timeLimit, TimeUnit.SECONDS)) match {
      case Success(x) => {
        //todo log
        true
      }
      case Failure(e) => {
        //todo log
        false
      }
    }
  }

  override def asyncSink(source: V): Future[Boolean] = {
    Future {
      sink(source)
    }
  }

  def buildRecord(source: V): ProducerRecord[String, V] = {
    new ProducerRecord[String, V](topic, source)
  }
}

object KafkaSinkFunc {

  val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  def buildKafkaProducer[K, V](kafkaBean: KafkaBean): KafkaProducer[K, V] = {
    val brokerList = kafkaBean.brokerList
    val serializerClass = kafkaBean.serializerClass
    val partitionerClass = kafkaBean.serializerClass
    val requiredAcks = kafkaBean.requiredAcks
    val props = new Properties()
    props.setProperty("bootstrap.servers", brokerList)
    props.setProperty("key.serializer", serializerClass)
    props.setProperty("partitioner.class", partitionerClass)
    props.setProperty("request.required.acks", requiredAcks)
    new KafkaProducer[K, V](props)
  }

}
