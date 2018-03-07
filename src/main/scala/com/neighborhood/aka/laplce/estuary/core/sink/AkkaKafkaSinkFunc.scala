package com.neighborhood.aka.laplce.estuary.core.sink

import akka.kafka.{ProducerMessage, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Sink, Source}
import com.neighborhood.aka.laplce.estuary.bean.datasink.KafkaBean
import com.neighborhood.aka.laplce.estuary.bean.key.BaseDataJsonKey
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Created by john_liu on 2018/3/7.
  */
class AkkaKafkaSinkFunc[K<:BaseDataJsonKey,V](kafkaBean: KafkaBean) extends AbstractKafkaSinkFunc[K,V](kafkaBean){

  def x = {

  }

}
