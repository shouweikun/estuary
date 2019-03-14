package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.record

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, FetcherMessage, SinkerMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataPositionRecorder
import com.neighborhood.aka.laplace.estuary.core.task.{PositionHandler, TaskManager}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.record.OplogRecorderCommand.{OplogRecorderEnsurePosition, OplogRecorderSaveLatestPosition, OplogRecorderSavePosition}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.OplogSinkerCommand.OplogSinkerGetAbnormal
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset

/**
  * oplog的位置管理Actor
  *
  * @param taskManager 任务管理区
  */
final class OplogPositionRecorder(
                                   override val taskManager: TaskManager
                                 ) extends SourceDataPositionRecorder[MongoOffset] {

  val logPositionHandler: PositionHandler[MongoOffset] = taskManager.positionHandler.asInstanceOf[PositionHandler[MongoOffset]]

  val isProfiling = taskManager.isProfiling

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId
  /**
    * 任务开始位置
    *
    * @return
    */
  override lazy val startPosition: Option[MongoOffset] = {
    while (getSaveOffset.isEmpty) {
      Thread.sleep(200)
    }
    getSaveOffset
  }

  override protected def saveOffsetInternal(offset: MongoOffset): Unit = {
    logPositionHandler.persistLogPosition(syncTaskId, offset)
  }

  override protected def getSaveOffset: Option[MongoOffset] = Option(logPositionHandler.getlatestIndexBy(syncTaskId))

  override protected def setProfilingContent: Unit = {
    lazy val latest: String = getLatestOffset.map(_.formatString).getOrElse("")
    lazy val last = getLastOffset.map(_.formatString).getOrElse("")
    lazy val scheduled = getScheduledSavedOffset.map(_.formatString).getOrElse("")
    lazy val scheduling = getSchedulingSavedOffset.map(_.formatString).getOrElse("")
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


  override def receive: Receive = {
    case BatcherMessage(x: MongoOffset) => updateOffset(x)
    case SyncControllerMessage(OplogRecorderSavePosition) => saveOffset
    case SinkerMessage(OplogSinkerGetAbnormal(e, offset)) => saveOffsetWhenError(e, offset)
    case SinkerMessage(OplogRecorderEnsurePosition(offset)) =>
    case FetcherMessage(OplogRecorderSaveLatestPosition) => saveLatestOffset
    case OplogRecorderSaveLatestPosition => saveLatestOffset
    case x: MongoOffset => updateOffset(x)
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

object OplogPositionRecorder {
  def props(taskManager: TaskManager): Props = Props(new OplogPositionRecorder(taskManager))
}