package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.mappingFormat

import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoConnection

/**
  * Created by john_liu on 2019/2/28.
  *
  * @neighborhood.aka.laplace
  */
final class Oplog2KafkaMessageMappingFormat(
                                             override val mongoConnection: MongoConnection,
                                             override val syncTaskId: String
                                           ) extends OplogMappingFormat[KafkaMessage] {


  override def transform(x: lifecycle.OplogClassifier): KafkaMessage = ???
}
