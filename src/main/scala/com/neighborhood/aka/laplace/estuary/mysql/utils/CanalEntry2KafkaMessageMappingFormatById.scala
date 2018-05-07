package com.neighborhood.aka.laplace.estuary.mysql.utils

import akka.actor.ActorLogging
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat

/**
  * Created by john_liu on 2018/5/7.
  */
trait CanalEntry2KafkaMessageMappingFormatById
  extends MappingFormat[CanalEntry.Entry, Array[KafkaMessage]] {
  self: ActorLogging =>
  override def transform(x: CanalEntry.Entry): Array[KafkaMessage] = {
    ???
  }
}
