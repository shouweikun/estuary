package com.neighborhood.aka.laplace.estuary.bean.datasink

import com.neighborhood.aka.laplace.estuary.bean.key.{BaseDataJsonKey, JsonKeySerializer, MultipleJsonKeyPartitioner}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * Created by john_liu on 2019/2/19.
  */
case class BaseDataJsonKeyKafkaBeanImp(
                         override val bootstrapServers: String,

                         /**
                           * defaultTopic
                           */
                         override val topic: String,

                         /**
                           * ddl专用Topic
                           */
                         override val ddlTopic: String
                       )(/**
                           * 分区类
                           */
                         override val partitionerClass: String = classOf[MultipleJsonKeyPartitioner].getName,
/**
  * key Serializer类
  */
override val keySerializer: String = classOf[JsonKeySerializer].getName,

/**
  * value Serializer类
  */
override val valueSerializer: String = classOf[StringSerializer].getName) extends KafkaBean[BaseDataJsonKey,String]