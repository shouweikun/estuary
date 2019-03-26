package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.hbase

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SinkerMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.sink.hbase.{HBaseSinkFunc, HBaseSinkManager}
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.OplogSinkerCommand.{OplogSinkerCheckFlush, OplogSinkerCollectOffset, OplogSinkerSendOffset, OplogSinkerStart}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.OplogSinkerEvent.OplogSinkerOffsetCollected
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset

/**
  * Created by john_liu on 2019/3/15.
  *
  * @note send by tableName
  */
class DefaultOplogKeyHBaseSinkerManager(
                                         override val taskManager: HBaseSinkManager with TaskManager
                                       ) extends SourceDataSinkerManagerPrototype[HBaseSinkFunc] {
  /**
    * sinker名称 用于动态加载
    */
  override val sinkerName: String = "sinker"

  val logEnabled = taskManager.logIsEnabled
  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId
  /**
    * position记录器
    */
  override lazy val positionRecorder: Option[ActorRef] = taskManager.positionRecorder

  /**
    * 是否是顶部
    */
  override val isHead: Boolean = true

  /**
    * 资源管理器
    *
    */
  override val sinkManager: SinkManager[HBaseSinkFunc] = taskManager

  /**
    * sinker的数量
    */
  override val sinkerNum: Int = taskManager.sinkerNum //但是没有生效

  val sink = sinkManager.sink

  val offsetMap: scala.collection.mutable.HashMap[String, MongoOffset] = new scala.collection.mutable.HashMap[String, MongoOffset]()


  def tableMutatorMap = sink.getAllHoldBufferMutator


  override def receive: Receive = {
    case SyncControllerMessage(OplogSinkerStart) => start
  }

  /**
    * 在线模式
    *
    * @return
    */
  override protected def online: Receive = {
    case OplogSinkerOffsetCollected(offset: MongoOffset) => handleOplogSinkerOffsetCollected(offset)
    case OplogSinkerSendOffset => handleOplogSinkerSendOffset
    case OplogSinkerCollectOffset => dispatchOplogSinkerCollectOffset
    case OplogSinkerCheckFlush => handleOplogCheckFlush
    case SinkerMessage(OplogSinkerCollectOffset) => dispatchOplogSinkerCollectOffset
    case SinkerMessage(OplogSinkerCheckFlush) => handleOplogCheckFlush
    case SinkerMessage(OplogSinkerSendOffset) => handleOplogSinkerSendOffset
    case SinkerMessage(OplogSinkerOffsetCollected(offset: MongoOffset)) => handleOplogSinkerOffsetCollected(offset)
    case SyncControllerMessage(OplogSinkerCollectOffset) => dispatchOplogSinkerCollectOffset
    case SyncControllerMessage(OplogSinkerCheckFlush) => handleOplogCheckFlush
    case SyncControllerMessage(OplogSinkerSendOffset) => handleOplogSinkerSendOffset
    case SyncControllerMessage(OplogSinkerOffsetCollected(offset: MongoOffset)) => handleOplogSinkerOffsetCollected(offset)
    case _ => //暂时不做其他处理
  }

  def handleOplogCheckFlush: Unit = {
    val ts = System.currentTimeMillis()
    if (logEnabled) log.info(s"start to handle check flush,id:$syncTaskId")
    val map = tableMutatorMap
    val values = map.values
    val keys = map.keySet
    values.foreach(_.flush())
    if (logEnabled) log.info(s"this flush cost is ${System.currentTimeMillis() - ts},tables:${keys.mkString(",")},id:$syncTaskId")
  }

  def handleOplogSinkerSendOffset: Unit = {
    if (logEnabled) log.info(s"start to send offset to recorder,id:$syncTaskId")
    val offset = offsetMap.values.toList match {
      case Nil => null
      case hd :: Nil => hd
      case list => list.reduce { (x: MongoOffset, y: MongoOffset) => x.compare(y, true) }
    }
    if (logEnabled) log.info(s"handleOplogSinkerSendOffset func get offset:${Option(offset).map(_.formatString).getOrElse("")},id:$syncTaskId")
    positionRecorder.foreach {
      ref =>
        Option(offset).map(x => ref ! x) //发送offset
        offsetMap.clear() //必须要清空
    }
  }

  def handleOplogSinkerOffsetCollected(offset: MongoOffset): Unit = {
    val senderName = sender().path.name
    offsetMap
      .get(senderName)
      .fold(offsetMap.put(senderName, offset)) {
        case odd => offsetMap.put(senderName, odd.compare(offset, true))
      }
  }

  def dispatchOplogSinkerCollectOffset: Unit = {
    if (logEnabled) log.info(s"dispatch oplog sinker collect offset to sinkers,id:$syncTaskId")
    context.children.foreach(ref => ref ! OplogSinkerCollectOffset)
  }

  /**
    * 初始化sinkers
    *
    *
    * 因为想用tableName作区分，所以其实initSinker阶段没有做事情
    */
  override protected def initSinkers: Unit = {
    log.info(s"DefaultOplogKeyHBaseSinkerManager start init sinkers,id:$syncTaskId")
    val sinkerList = (1 to sinkerNum).map(num => context.actorOf(SimpleHBasePutSinker.props(taskManager, num).withDispatcher("akka.sinker-dispatcher"))).toList
    taskManager.sinkerList = sinkerList //很重要
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

object DefaultOplogKeyHBaseSinkerManager {
  val name: String = DefaultOplogKeyHBaseSinkerManager.getClass.getName.stripSuffix("$")

  def props(taskManager: HBaseSinkManager with TaskManager): Props = Props(new DefaultOplogKeyHBaseSinkerManager(taskManager))
}

