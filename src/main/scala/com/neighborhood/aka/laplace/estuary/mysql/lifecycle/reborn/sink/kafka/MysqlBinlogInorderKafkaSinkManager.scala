package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.kafka

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.core.lifecycle.SyncControllerMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinkerCommand.MysqlInOrderSinkerStart
import com.neighborhood.aka.laplace.estuary.mysql.sink.BinlogKeyKafkaSInkManagerImp

/**
  * Created by john_liu on 2019/2/26.
  */
abstract class MysqlBinlogInorderKafkaSinkManager[K, V](
                                                         val taskManager: SinkManager[KafkaSinkFunc[K, V]] with TaskManager
                                                       ) extends SourceDataSinkerManagerPrototype[KafkaSinkFunc[K, V]] {

  //目前就一个开始方法
  override def receive: Receive = {
    case SyncControllerMessage(MysqlInOrderSinkerStart) => start
  }

  /**
    * 在线模式
    *
    * @return
    */
  override protected def online: Receive = {
    case _ => //暂时不做什么
  }
}

object MysqlBinlogInorderKafkaSinkManager {
  def buildMysqlBinlogInorderKafkaSinkManager(name: String, taskManager: SinkManager[KafkaSinkFunc[_, _]] with TaskManager): Props = name match {
    case MysqlBinlogInorderKafkaBinlogKeySinkManager.name => MysqlBinlogInorderKafkaBinlogKeySinkManager.props(taskManager.asInstanceOf[BinlogKeyKafkaSInkManagerImp with TaskManager])
    case _ => throw new RuntimeException(s"cannot find MysqlBinlogInorderKafka match name :$name")
  }
}