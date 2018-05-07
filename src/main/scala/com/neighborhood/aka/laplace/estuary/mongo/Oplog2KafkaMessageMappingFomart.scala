package com.neighborhood.aka.laplace.estuary.mongo

import akka.actor.ActorLogging
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat

/**
  * Created by john_liu on 2018/5/7.
  */
trait Oplog2KafkaMessageMappingFomart extends MappingFormat[oplog,KafkaMessage] {
//  self: ActorLogging =>
//
//}
