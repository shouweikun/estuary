package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import com.neighborhood.aka.laplace.estuary.bean.exception.sink.SinkerAbnormalException
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.PositionRecorder
import com.neighborhood.aka.laplace.estuary.core.offset.ComparableOffset
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by john_liu on 2019/1/10.
  * 这个是处理Offset的Actor，用于SavePoint保存
  */
trait SourceDataPositionRecorder[A <: ComparableOffset[A]] extends ActorPrototype with PositionRecorder {
  private val queneMaxSize = 10
  private lazy val quene: mutable.Queue[A] = new mutable.Queue[A]()
  lazy val logger = LoggerFactory.getLogger(classOf[SourceDataPositionRecorder[A]])

  protected def error: Receive = {
    case _ =>
  }

  def startPosition: Option[A]

  def destination: String = this.syncTaskId

  private var latestOffset: Option[A] = startPosition
  private var lastSavedOffset: Option[A] = startPosition
  private var scheduledSavedOffset: Option[A] = startPosition
  private var schedulingSavedOffset: Option[A] = startPosition

  def updateOffset(offset: A): Unit
  = {
    latestOffset = Option(offset)
    setProfilingContent
  }

  protected def saveOffset: Unit = {
    logger.info(s"start saveOffset,id:$syncTaskId")
    scheduledSavedOffset.map(saveOffsetInternal(_))
    scheduledSavedOffset.map(updateQuene(_))
    scheduledSavedOffset = schedulingSavedOffset
    schedulingSavedOffset = lastSavedOffset
    lastSavedOffset = latestOffset
  }

  protected def saveOffsetWhenError(e: Throwable, offset: Option[A]): Unit = {
    offset.flatMap(getMatchOffset(_)).fold(log.warning(s"this can be really dangerous,cause cannot find a suitable offset to save when error, considering of loss of data plz,id:$syncTaskId"))(saveOffsetInternal(_))
    context.become(error)
    throw new SinkerAbnormalException(s"sinker some thing wrong,e:$e,message:${e.getMessage},id:$syncTaskId")
  }

  protected def saveOffsetInternal(offset: A): Unit

  protected def getSaveOffset: Option[A]

  protected def getLatestOffset: Option[A] = latestOffset

  protected def getLastOffset: Option[A] = lastSavedOffset

  protected def getScheduledSavedOffset: Option[A] = scheduledSavedOffset

  protected def getSchedulingSavedOffset: Option[A] = schedulingSavedOffset

  protected def setProfilingContent: Unit

  private def updateQuene(offset: A): Unit = {
    quene.enqueue(offset)
    if (quene.size > queneMaxSize) quene.dequeue()
  }

  private def getMatchOffset(offset: A): Option[A] = quene.reverse.dequeueFirst(offset.compare(_))
}
