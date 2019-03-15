package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.kafka

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.OplogSinkerCommand
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.slf4j.LoggerFactory
import OplogSinkCallback._

/**
  * Created by john_liu on 2019/3/5.
  */
class OplogSinkCallback(
                         val abnormal: AtomicBoolean,
                         val positionRecorder: ActorRef,
                         val mongoOffset: MongoOffset
                       ) extends Callback {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null && abnormal.compareAndSet(false, true)) {
      logger.warn(s"exception happened:$exception,message:${exception.getMessage}")
      positionRecorder ! OplogSinkerCommand.OplogSinkerGetAbnormal(exception, Option(mongoOffset))
    }
  }
}

object OplogSinkCallback {
  private[OplogSinkCallback] val logger = LoggerFactory.getLogger(classOf[OplogSinkCallback])
}
