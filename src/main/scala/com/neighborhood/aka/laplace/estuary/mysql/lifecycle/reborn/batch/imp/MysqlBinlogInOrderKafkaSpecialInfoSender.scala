package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderSpecialInfoSender
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp

/**
  * Created by john_liu on 2019/1/15.
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInOrderKafkaSpecialInfoSender(
                                                      override val taskManager: MysqlSourceManagerImp with TaskManager,
                                                      override val sinker: ActorRef) extends MysqlBinlogInOrderSpecialInfoSender[KafkaMessage](sinker, taskManager) {


  /**
    *
    * 这是一个不好的实现，因为将sink和本类耦合了
    * 并且并*没有*发送给Sinker,而是直接发送给Mysql了
    *
    * @param dbNameList 需要发送dummyData的db名称列表
    * @param sinker     sinker的ActorRef
    *                   构造假数据并发送给sinker
    */
  override protected def buildAndSendDummyHeartbeatMessage(dbNameList: Iterable[String])(sinker: ActorRef): Unit = {

  }
}

object MysqlBinlogInOrderKafkaSpecialInfoSender {
  def props(taskManager: MysqlSourceManagerImp with TaskManager,
            sinker: ActorRef): Props = Props(new MysqlBinlogInOrderKafkaSpecialInfoSender(taskManager, sinker))
}