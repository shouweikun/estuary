package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.MysqlRowDataInfo
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderSpecialInfoSender
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp

import scala.util.Try

/**
  * Created by john_liu on 2019/1/15.
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInOrderMysqlSpecialInfoSender(
                                                      override val taskManager: MysqlSinkManagerImp with MysqlSourceManagerImp with TaskManager,
                                                      override val sinker: ActorRef) extends MysqlBinlogInOrderSpecialInfoSender[MysqlRowDataInfo](sinker, taskManager) {

  /**
    * 心跳表名称
    */
  private val heartBeatCheckTableName: String = "" //todo
  /**
    * mysqlSinkFunc
    */
  private val sink: MysqlSinkFunc = taskManager.sink

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
    lazy val sql: String = s"$heartBeatCheckTableName" //todo
    if (!sink.isTerminated) Try(sink.insertSql(sql)) //绝对不要扔出异常！！！！
  }


}

object MysqlBinlogInOrderMysqlSpecialInfoSender {
  val name: String = MysqlBinlogInOrderMysqlSpecialInfoSender.getClass.getName.stripSuffix("$")

  def props(taskManager: MysqlSinkManagerImp with MysqlSourceManagerImp with TaskManager,
            sinker: ActorRef): Props = Props(new MysqlBinlogInOrderMysqlSpecialInfoSender(taskManager, sinker))
}