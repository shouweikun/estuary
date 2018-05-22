package com.neighborhood.aka.laplace.estuary.core.sink

import com.neighborhood.aka.laplace.estuary.bean.datasink.KafkaBean
import com.neighborhood.aka.laplace.estuary.bean.key.BaseDataJsonKey

import scala.concurrent.Future

/**
  * Created by john_liu on 2018/3/7.
  */
class AkkaKafkaSinkFunc[K<:BaseDataJsonKey,V](kafkaBean: KafkaBean) extends AbstractKafkaSinkFunc[K,V](kafkaBean){



}
