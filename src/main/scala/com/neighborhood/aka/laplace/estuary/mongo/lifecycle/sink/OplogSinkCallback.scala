package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

/**
  * Created by john_liu on 2019/3/5.
  */
class OplogSinkCallback extends Callback{
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = ???
}
