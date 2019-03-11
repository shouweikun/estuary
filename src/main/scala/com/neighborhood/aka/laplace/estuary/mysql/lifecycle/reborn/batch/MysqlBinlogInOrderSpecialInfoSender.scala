package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.bean.exception.other.WorkerInitialFailureException
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.BatcherMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSpecialBatcherPrototype
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderBatcherCommand.MysqlBinlogInOrderBatcherCheckHeartbeats
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderBatcherEvent.MysqlBinlogInOrderBatcherHeartbeatsChecked
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp.{MysqlBinlogInOrderMysqlSpecialInfoSender, MysqlBinlogInOrderMysqlSpecialInfoSender4Sda}
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp
import com.neighborhood.aka.laplace.estuary.mysql.task.mysql.Mysql2MysqlTaskInfoManager

/**
  * Created by john_liu on 2019/1/9.
  * 抽象的MysqlBinlogInOrderSpecialInfoSender
  *
  * @tparam R 是结果类型,对应MappingFormat的tparam R
  * @note 当你实现一个新的MysqlBinlogInOrderSpecialInfoSender时，一定要在`buildMysqlBinlogInOrderSpecialInfoSenderByName`工厂中添加相应类型
  * @since 2019-01-09
  * @author nbhd.aka.neighborhood
  */
abstract class MysqlBinlogInOrderSpecialInfoSender[R](
                                                       override val sinker: ActorRef,
                                                       override val taskManager: MysqlSourceManagerImp with TaskManager
                                                     ) extends SourceDataSpecialBatcherPrototype {
  /**
    * 是否发送心跳
    */
  val isSendingHeartbeats: Boolean = taskManager.isSendingHeartbeat
  /**
    * 事件收集器
    */
  override lazy val eventCollector: Option[ActorRef] = taskManager.eventCollector

  /**
    * position记录器
    */
  protected lazy val positionRecorder: Option[ActorRef] = taskManager.positionRecorder
  /**
    * 该mysql实例所有的db
    */
  private lazy val mysqlDatabaseNameList = taskManager.mysqlDatabaseNameList
  /**
    * 需要发送心跳的数据库名称
    */
  val concernedDbName = taskManager.concernedDatabase
  /**
    * 不需要发送心跳的数据库名称
    */
  val ignoredDbName = taskManager.ignoredDatabase

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

  /**
    * 错位次数阈值
    */
  override def errorCountThreshold: Int = 1

  /**
    * 错位次数
    */
  override var errorCount: Int = 0
  /**
    * 当前binlogPositionInfo
    */
  protected var currentBinlogPositionInfo: Option[BinlogPositionInfo] = None

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???

  override def receive: Receive = {
    case BatcherMessage(x: BinlogPositionInfo) => sendBinlogPositionInfoAndRecord(x)
    case BatcherMessage(MysqlBinlogInOrderBatcherCheckHeartbeats) => sendHeartBeats
    case x: BinlogPositionInfo => sendBinlogPositionInfoAndRecord(x)
  }

  /**
    * 发送BinlogPositionInfo
    *
    * @param x
    */
  private def sendBinlogPositionInfoAndRecord(x: BinlogPositionInfo): Unit = {
    currentBinlogPositionInfo = Option(x)
    positionRecorder
      .fold(log.warning(s"cannot find positionRecorder when sending BinlogPositionInfo ,id:$syncTaskId"))(ref => ref ! BatcherMessage(x))
  }

  /**
    * 发送心跳
    */
  protected def sendHeartBeats: Unit = {
    val concernedDbNames = concernedDbName
    val ignoredDbNames = ignoredDbName
    log.info(s"try to send heartbeats,concernList:${concernedDbNames.mkString(",")},ignored:${ignoredDbName.mkString(",")},id:$syncTaskId")
    (concernedDbNames.size > 0, ignoredDbNames.size > 0) match {
      case (true, _) => buildAndSendDummyHeartbeatMessage(concernedDbNames)(sinker)
      case (_, true) => buildAndSendDummyHeartbeatMessage(mysqlDatabaseNameList.diff(ignoredDbNames))(sinker)
      case (_, false) => buildAndSendDummyHeartbeatMessage(mysqlDatabaseNameList)(sinker)
    }

    eventCollector.map(ref => ref ! MysqlBinlogInOrderBatcherHeartbeatsChecked)

  }

  /**
    *
    * @param dbNameList 需要发送dummyData的db名称列表
    * @param sinker     sinker的ActorRef
    *                   构造假数据并发送给sinker
    */
  protected def buildAndSendDummyHeartbeatMessage(dbNameList: Iterable[String])(sinker: ActorRef): Unit

  override def preStart(): Unit = {
    log.debug(s"init MysqlBinlogInOrderSpecialInfoSender,id:$syncTaskId")

  }

  override def postStop(): Unit = {
    log.info(s"MysqlBinlogInOrderSpecialInfoSender process postStop,id:$syncTaskId")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"MysqlBinlogInOrderSpecialInfoSender process preRestart,id:$syncTaskId")

  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"MysqlBinlogInOrderSpecialInfoSender process postRestart,id:$syncTaskId")

  }
}

object MysqlBinlogInOrderSpecialInfoSender {
  /**
    * 构建MysqlBinlogInOrderSpecialInfoSender的工厂方法
    * 当你增加新的MysqlBinlogInOrderSpecialInfoSender时，一定要在这个工厂方法里增加
    *
    * @param name MysqlBinlogInOrderSpecialInfoSender的名字
    * @return Props
    */
  def buildMysqlBinlogInOrderSpecialInfoSender(name: String, taskManager: MysqlSourceManagerImp with TaskManager, sinker: ActorRef): Props = name match {
    case MysqlBinlogInOrderMysqlSpecialInfoSender.name => MysqlBinlogInOrderMysqlSpecialInfoSender.props(taskManager.asInstanceOf[Mysql2MysqlTaskInfoManager], sinker)
    case MysqlBinlogInOrderMysqlSpecialInfoSender4Sda.name => MysqlBinlogInOrderMysqlSpecialInfoSender4Sda.props(taskManager.asInstanceOf[Mysql2MysqlTaskInfoManager], sinker)
    case _ => throw new WorkerInitialFailureException(s"cannot build MysqlBinlogInOrderSpecialInfoSender name item match $name")
  }
}