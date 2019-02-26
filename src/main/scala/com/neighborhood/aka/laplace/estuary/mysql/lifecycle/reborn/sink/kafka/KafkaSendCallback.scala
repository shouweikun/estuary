package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.kafka

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.core.lifecycle.SinkerMessage
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinkerCommand.MysqlInOrderSinkerGetAbnormal
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

/**
  * Created by john_liu on 2019/2/26.
  *
  * @author neighborhood.aka.laplace
  */
final class KafkaSendCallback(
                               val positionRecorder: Option[ActorRef],
                               val sinker: ActorRef,
                               binlogPosition: => Option[BinlogPositionInfo] = None
                             ) extends Callback {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    lazy val errorMessage = SinkerMessage(MysqlInOrderSinkerGetAbnormal(exception, binlogPosition))
    if (exception != null) {
      positionRecorder.map(ref => ref ! errorMessage)
      sinker ! errorMessage
    }
  }
}
