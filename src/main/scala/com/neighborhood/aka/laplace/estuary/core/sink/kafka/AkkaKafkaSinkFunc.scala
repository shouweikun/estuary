package com.neighborhood.aka.laplace.estuary.core.sink.kafka

import com.neighborhood.aka.laplace.estuary.bean.datasink.KafkaBean
import com.neighborhood.aka.laplace.estuary.bean.key.BaseDataJsonKey

/**
  * Created by john_liu on 2018/3/7.
  */
class AkkaKafkaSinkFunc[K<:BaseDataJsonKey,V](kafkaBean: KafkaBean) extends AbstractKafkaSinkFunc[K,V](kafkaBean){



}
