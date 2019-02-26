package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.mysql

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinkerManager
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp

/**
  * Created by john_liu on 2019/1/14.
  * MysqlSink的SinkManager
  *
  * @note 需要提供:
  *       $sinkerName  默认加载Mysql2Mysql
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInOrderMysqlSinkerManager(override val taskManager: MysqlSinkManagerImp with TaskManager) extends MysqlBinlogInOrderSinkerManager(taskManager) {


  /**
    * position记录器
    */
  override lazy val positionRecorder: Option[ActorRef] = taskManager.positionRecorder

  override val sinkerName: String = "sinker"

  /**
    * 初始化sinkers
    */
  override protected def initSinkers: Unit = {
    val sinkTypeName = sinkerNameToLoad.get(sinkerName).getOrElse(MysqlBinlogInOrderMysqlSinker.name)
    log.info(s"MysqlBinlogInOrderMysqlSinkerManager init sinkers,sinkerName:$sinkTypeName,num:$sinkerNum,id:$syncTaskId")
    val sinkerList = (1 to sinkerNum).map(index => MysqlBinlogInOrderSinker.buildMysqlBinlogInOrderSinker(sinkTypeName, taskManager, index).withDispatcher("akka.sinker-dispatcher")).map(context.actorOf(_)).toList
    taskManager.sinkerList = sinkerList
    log.info(s"sinkList has been updated into taskManager,id:$syncTaskId")
  }
}

object MysqlBinlogInOrderMysqlSinkerManager {
  lazy val name = MysqlBinlogInOrderMysqlSinkerManager.getClass.getName.stripSuffix("$")

  def props(taskManager: MysqlSinkManagerImp with TaskManager): Props = Props(new MysqlBinlogInOrderMysqlSinkerManager(taskManager))
}