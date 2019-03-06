package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.record

import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataPositionRecorder
import com.neighborhood.aka.laplace.estuary.core.task.{PositionHandler, TaskManager}
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset

/**
  * Created by john_liu on 2019/3/6.
  *
  * @todo finish me
  */
final class OplogPositionRecorder(
                                   override val taskManager: TaskManager
                                 ) extends SourceDataPositionRecorder[MongoOffset] {

  val logPositionHandler: PositionHandler[MongoOffset] = ???

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

  }

  override protected def getSaveOffset: Option[MongoOffset] = ???

  override protected def setProfilingContent: Unit = ???


  override def receive: Receive = ???

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
