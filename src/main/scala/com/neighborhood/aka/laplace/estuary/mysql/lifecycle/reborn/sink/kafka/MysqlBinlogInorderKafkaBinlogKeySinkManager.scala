package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.kafka

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinker
import com.neighborhood.aka.laplace.estuary.mysql.sink.BinlogKeyKafkaSInkManagerImp

/**
  * Created by john_liu on 2019/2/27.
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInorderKafkaBinlogKeySinkManager(
                                                         override val taskManager: BinlogKeyKafkaSInkManagerImp with TaskManager
                                                       ) extends MysqlBinlogInorderKafkaSinkManager[BinlogKey, String](taskManager) {

  /**
    * 同步任务id
    */
  override lazy val syncTaskId: String = taskManager.syncTaskId
  /**
    * sinker名称 用于动态加载
    */
  override val sinkerName: String = "sinker"

  /**
    * position记录器
    */
  override lazy val positionRecorder: Option[ActorRef] = taskManager.positionRecorder

  /**
    * 是否是顶部
    */
  override def isHead: Boolean = true

  /**
    * 资源管理器
    *
    */
  override lazy val sinkManager: SinkManager[KafkaSinkFunc[BinlogKey, String]] = taskManager

  /**
    * sinker的数量
    */
  override lazy val sinkerNum: Int = taskManager.sinkerNum


  /**
    * 初始化sinkers
    */
  override protected def initSinkers: Unit = {
    log.info(s"MysqlBinlogInorderKafkaBinlogKeySinkManager try to start init sinkers,id:$syncTaskId")
    val sinkTypeName = sinkerNameToLoad.get(sinkerName).getOrElse(MysqlBinlogInorderKafkaSinker.name)
    log.info(s"MysqlBinlogInOrderMysqlSinkerManager init sinkers,sinkerName:$sinkTypeName,num:$sinkerNum,id:$syncTaskId")
    val sinkerList = (1 to sinkerNum).map(index => MysqlBinlogInOrderSinker.buildMysqlBinlogInOrderSinker(sinkTypeName, taskManager, index).withDispatcher("akka.sinker-dispatcher")).map(context.actorOf(_)).toList
    taskManager.sinkerList = sinkerList
    log.info(s"sinkList has been updated into taskManager,id:$syncTaskId")
  }

  /**
    * 错位次数阈值
    */
  override def errorCountThreshold: Int = 0

  /**
    * 错位次数
    */
  override var errorCount: Int = 0

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???


}

object MysqlBinlogInorderKafkaBinlogKeySinkManager {
  val name = MysqlBinlogInorderKafkaBinlogKeySinkManager.getClass.getName.stripSuffix("$")

  def props(taskManager: BinlogKeyKafkaSInkManagerImp with TaskManager): Props = Props(new MysqlBinlogInorderKafkaBinlogKeySinkManager(taskManager))
}