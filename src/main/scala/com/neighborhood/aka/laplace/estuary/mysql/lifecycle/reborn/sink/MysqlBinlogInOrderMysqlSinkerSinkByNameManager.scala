package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, SinkerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.MysqlRowDataInfo
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinkerCommand.MysqlInOrderSinkerGetAbnormal
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp

import scala.util.Try

/**
  * Created by john_liu on 2019/1/14.
  * MysqlSink的SinkManagerSinkByName
  *
  * 这个设计其实是不好的，因为要兼容原来的体系
  * 通过库表名进行分发
  *
  * @note 需要提供:
  *       $sinkerName  默认加载Mysql2Mysql
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInOrderMysqlSinkerSinkByNameManager(override val taskManager: MysqlSinkManagerImp with TaskManager) extends MysqlBinlogInOrderSinkerManager(taskManager) {

  /**
    * position记录器
    */
  override lazy val positionRecorder: Option[ActorRef] = taskManager.positionRecorder

  override val sinkerName: String = "sinker"

  private val sinkTypeName = sinkerNameToLoad.get(sinkerName).getOrElse(MysqlBinlogInOrderMysqlSinker.name)
  private var sinkerNumInc = 0

  override def online: Receive = {
    case x@SinkerMessage(_: MysqlInOrderSinkerGetAbnormal) => send2Recorder(x)
    case m@BatcherMessage(x: MysqlRowDataInfo) => getOrCreateSinker(s"${x.dbName}.${x.tableName}") ! m
  }

  /**
    * 初始化sinkers
    *
    * 目前的做法是让自己成为对接batcher的Sinker，然后内部转发
    */
  override protected def initSinkers: Unit = {
    log.info(s"MysqlBinlogInOrderMysqlSinkByNameSinkerManager init sinkers,sinkerName:$sinkTypeName,id:$syncTaskId")
    log.warning(s"Sink by name currently is a special case,which is not a good design,considering a new imp,id:$syncTaskId")
    val sinkerList = (1 to sinkerNum).map(_ => self).toList
    taskManager.sinkerList = sinkerList
    log.info(s"sinkList has been updated into taskManager,id:$syncTaskId")
  }

  /**
    * 通过db.tb获取sinker
    *
    * @param dbAndTbName
    * @return
    */
  private def getOrCreateSinker(dbAndTbName: String): ActorRef = context.child(dbAndTbName).getOrElse {
    val index = sinkerNumInc
    sinkerNumInc = sinkerNumInc + 1
    context.actorOf(MysqlBinlogInOrderSinker.buildMysqlBinlogInOrderSinker(sinkTypeName, taskManager, index).withDispatcher("akka.sinker-dispatcher"), dbAndTbName)
  }
}

object MysqlBinlogInOrderMysqlSinkerSinkByNameManager {
  val name: String = MysqlBinlogInOrderMysqlSinkerSinkByNameManager.getClass.getName.stripSuffix("$")

  def props(taskManager: MysqlSinkManagerImp with TaskManager): Props = Props(new MysqlBinlogInOrderMysqlSinkerSinkByNameManager(taskManager))
}
