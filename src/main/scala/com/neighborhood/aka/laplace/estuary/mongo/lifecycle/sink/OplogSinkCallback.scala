package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink

import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.slf4j.LoggerFactory
import OplogSinkCallback._

/**
  * Created by john_liu on 2019/3/5.
  */
class OplogSinkCallback extends Callback {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null) logger.warn(s"exception happened:$exception,message:${exception.getMessage}")
  }
}

object OplogSinkCallback {
  private[OplogSinkCallback] val logger = LoggerFactory.getLogger(classOf[OplogSinkCallback])
}
