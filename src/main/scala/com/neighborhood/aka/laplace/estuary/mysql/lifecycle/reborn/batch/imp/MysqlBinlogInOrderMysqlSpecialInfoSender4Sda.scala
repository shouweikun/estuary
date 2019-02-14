package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{BinlogPositionInfo, MysqlRowDataInfo}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderSpecialInfoSender
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp

/**
  * Created by john_liu on 2019/1/15.
  */
final class MysqlBinlogInOrderMysqlSpecialInfoSender4Sda(
                                                          override val taskManager: MysqlSinkManagerImp with MysqlSourceManagerImp with TaskManager,
                                                          override val sinker: ActorRef) extends MysqlBinlogInOrderSpecialInfoSender[MysqlRowDataInfo](sinker, taskManager) {

  /**
    * 心跳表名称
    */
  private val heartBeatCheckTableNames: List[String] = taskManager.concernedDatabase.map(x => s"heartbeat.${x}_mysql")
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
    val binlogPositionInfo = this.currentBinlogPositionInfo.getOrElse(BinlogPositionInfo("", 0, 0))
    heartBeatCheckTableNames
      .map { tableName => s"replace into $tableName (id,create_time,consume_position) VALUES(1,NOW(),'$binlogPositionInfo')" }
      .map(sql => if (!sink.isTerminated) sink.insertSql(sql)) //绝对要扔出异常！！！！)
  }
}

object MysqlBinlogInOrderMysqlSpecialInfoSender4Sda {
  val name: String = MysqlBinlogInOrderMysqlSpecialInfoSender4Sda.getClass.getName.stripSuffix("$")

  def props(taskManager: MysqlSinkManagerImp with MysqlSourceManagerImp with TaskManager,
            sinker: ActorRef): Props = Props(new MysqlBinlogInOrderMysqlSpecialInfoSender4Sda(taskManager, sinker))
}