package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

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

  def saveOffset: Unit = {
    logger.info(s"start saveOffset,id:$syncTaskId")
    scheduledSavedOffset.map(saveOffsetInternal(_))
    scheduledSavedOffset.map(updateQuene(_))
    scheduledSavedOffset = schedulingSavedOffset
    schedulingSavedOffset = lastSavedOffset
    lastSavedOffset = latestOffset
  }

  def saveOffsetWhenError(e: Throwable, offset: Option[A]): Unit = {
    val oldest: Option[A] = quene.headOption
    offset.flatMap(getMatchOffset(_)).fold {
      log.warning(s"this can be really dangerous,cause cannot find a suitable offset to save when error, considering of loss of data plz,id:$syncTaskId")
      oldest.map(saveOffsetInternal(_)) //把最老的保存一下
      if (oldest.isDefined) log.warning(s"try to save the oldest offset instead,but still can not guarantee no data is loss,id:$syncTaskId ")
    }(saveOffsetInternal(_))

    context.become(error)
    throw new RuntimeException(s"some thing wrong,e:$e,message:${e.getMessage},id:$syncTaskId")
  }

  /**
    * 这是一个特殊情况
    * 会被特殊的事件触发
    * 效果是清除所有的缓存offset,保留latest offset
    */
  def saveLatestOffset: Unit = {
    log.warning(s"start to save latest offset,id:$syncTaskId")
    lastSavedOffset = latestOffset
    schedulingSavedOffset = latestOffset
    scheduledSavedOffset = latestOffset
    quene.clear() //将队列清空
    saveOffset
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
