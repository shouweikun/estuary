package com.neighborhood.aka.laplace.estuary.core.sink

import akka.kafka.{ProducerMessage, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Sink, Source}
import com.neighborhood.aka.laplace.estuary.bean.datasink.KafkaBean
import com.neighborhood.aka.laplace.estuary.bean.datasink.KafkaBean
import com.neighborhood.aka.laplace.estuary.bean.key.BaseDataJsonKey
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Future

/**
  * Created by john_liu on 2018/3/7.
  */
class AkkaKafkaSinkFunc[K<:BaseDataJsonKey,V](kafkaBean: KafkaBean) extends AbstractKafkaSinkFunc[K,V](kafkaBean){

  def x = {
       Future{1}.map(x=>x)
  }

}
