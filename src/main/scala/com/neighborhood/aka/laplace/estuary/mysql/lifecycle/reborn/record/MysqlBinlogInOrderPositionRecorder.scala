package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.record

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataPositionRecorder
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, FetcherMessage, SinkerMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.record.MysqlBinlogInOrderRecorderCommand.{MysqlBinlogInOrderRecorderEnsurePosition, MysqlBinlogInOrderRecorderSaveLatestPosition, MysqlBinlogInOrderRecorderSavePosition}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinkerCommand.MysqlInOrderSinkerGetAbnormal
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp
import com.neighborhood.aka.laplace.estuary.mysql.utils.LogPositionHandler

import scala.util.Try

/**
  * Created by john_liu on 2019/1/10.
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInOrderPositionRecorder(
                                                override val taskManager: MysqlSourceManagerImp with TaskManager) extends SourceDataPositionRecorder[BinlogPositionInfo] {
  //for 隐式转换
  import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo._

  /**
    * logPosition管理器
    */
  private lazy val logPositionHandler: LogPositionHandler = taskManager.logPositionHandler

  private val isProfiling: Boolean = taskManager.isProfiling

  /**
    * 任务开始位置
    *
    *
    * @return
    */
  lazy val startPosition: Option[BinlogPositionInfo] = {
    while (getSaveOffset.isEmpty) {
      Thread.sleep(200)
    }
    getSaveOffset
  }


  override protected def saveOffsetInternal(offset: BinlogPositionInfo): Unit = Try {
    log.info(s"start save offset internal,offset:$offset,id:$syncTaskId")
    logPositionHandler.persistLogPosition(destination, offset.toLogPosition)
  }.failed.foreach(e => log.warning(s"cannot save logPosition,cause:$e,message:${e.getMessage},id:$syncTaskId"))

  override protected def getSaveOffset: Option[BinlogPositionInfo] = Try(logPositionHandler.getLatestIndexBy(destination).toBinlogPositionInfo).toOption.flatMap(Option(_))


  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

  override def receive: Receive = {
    case BatcherMessage(x: BinlogPositionInfo) => updateOffset(x)
    case SyncControllerMessage(MysqlBinlogInOrderRecorderSavePosition) => saveOffset
    case SinkerMessage(MysqlInOrderSinkerGetAbnormal(e, offset)) => saveOffsetWhenError(e, offset)
    case SinkerMessage(MysqlBinlogInOrderRecorderEnsurePosition(offset)) =>
    case FetcherMessage(MysqlBinlogInOrderRecorderSaveLatestPosition) => saveLatestOffset
    case MysqlBinlogInOrderRecorderSaveLatestPosition => saveLatestOffset
    case x: BinlogPositionInfo => updateOffset(x)
  }

  protected def setProfilingContent: Unit = {
    lazy val latest: String = getLatestOffset.map { offset => s"${offset.journalName}:${offset.offset}:${offset.timestamp}" }.getOrElse("")
    lazy val last = getLastOffset.map { offset => s"${offset.journalName}:${offset.offset}:${offset.timestamp}" }.getOrElse("")
    lazy val scheduled = getScheduledSavedOffset.map { offset => s"${offset.journalName}:${offset.offset}:${offset.timestamp}" }.getOrElse("")
    lazy val scheduling = getSchedulingSavedOffset.map { offset => s"${offset.journalName}:${offset.offset}:${offset.timestamp}" }.getOrElse("")
    if (isProfiling) taskManager
      .sinkerLogPosition.set(
      s"""
        {
          "syncTaskId":"$syncTaskId",
           "latestBinlog":"$latest",
           "lastSavePoint":" $last",
           "schedulingSavePoint":"$scheduling",
           "scheduledSavePoint":"$scheduled"
        }
         """.stripMargin)
  }


  /**
    * 错位次数阈值
    */
  override val errorCountThreshold: Int = 1
  /**
    * 错位次数
    */
  override var errorCount: Int = 0

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???
}

object MysqlBinlogInOrderPositionRecorder {
  def props(
             taskManager: MysqlSourceManagerImp with TaskManager
           ): Props = Props(new MysqlBinlogInOrderPositionRecorder(taskManager))
}
