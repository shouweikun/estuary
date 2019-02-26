package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.EntryKeyClassifier
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderBatcher
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp

/**
  * Created by john_liu on 2019/1/15.
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInOrderKafkaBatcher(override val taskManager: MysqlSourceManagerImp with TaskManager,
                                           override val sinker: ActorRef,
                                           override val num: Int) extends MysqlBinlogInOrderBatcher[KafkaMessage](taskManager, sinker, num) {

  override val mappingFormat: MappingFormat[EntryKeyClassifier, KafkaMessage] = taskManager.batchMappingFormat.get.asInstanceOf[MappingFormat[EntryKeyClassifier, KafkaMessage]]

}

object MysqlBinlogInOrderKafkaBatcher {
  def props(
             taskManager: MysqlSourceManagerImp with TaskManager,
             sinker: ActorRef,
             num: Int
           ): Props = Props(new MysqlBinlogInOrderKafkaBatcher(taskManager, sinker, num))
}


