package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.hdfs


import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.bean.support.HdfsMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, SinkerMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.sink.hdfs.HdfsSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.OplogSinkerCommand.{OplogSinkerCheckFlush, OplogSinkerCollectOffset, OplogSinkerSendOffset, OplogSinkerStart}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.OplogSinkerEvent.OplogSinkerOffsetCollected
import com.neighborhood.aka.laplace.estuary.mongo.sink.hdfs.HdfsSinkManagerImp
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset
import com.neighborhood.aka.laplace.estuary.mongo.task.hdfs.Mongo2HdfsTaskInfoManager

/**
  * Created by john_liu on 2019/3/2.
  *
  * @author neighborhood.aka.laplace
  */
final class OplogKeyHdfsByNameSinkerManager(
                                             override val taskManager: HdfsSinkManagerImp with TaskManager
                                           ) extends SourceDataSinkerManagerPrototype[HdfsSinkFunc] {
  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId
  /**
    * sinker名称 用于动态加载
    */
  override val sinkerName: String = "sinker"

  /**
    * position记录器
    */
  override lazy val positionRecorder: Option[ActorRef] = taskManager.positionRecorder
  val offsetMap: scala.collection.mutable.HashMap[String, MongoOffset] = new scala.collection.mutable.HashMap[String, MongoOffset]()

  /**
    * 是否是顶部
    */
  override def isHead: Boolean = true


  /**
    * 资源管理器
    *
    */
  override lazy val sinkManager: SinkManager[HdfsSinkFunc] = taskManager

  /**
    * sinker的数量
    */
  override val sinkerNum: Int = taskManager.sinkerNum

  override def receive: Receive = {
    case SyncControllerMessage(OplogSinkerStart) => start
  }

  /**
    * 在线模式
    *
    * @return
    */
  override protected def online: Receive = {
    case x: HdfsMessage[MongoOffset] => getOrCreateSinker(s"${x.dbName}.${x.tableName}") ! x
    case BatcherMessage(x: HdfsMessage[MongoOffset]) => getOrCreateSinker(s"${x.dbName}.${x.tableName}") ! x
    case SinkerMessage(OplogSinkerSendOffset) => handleOplogSinkerSendOffset
    case SinkerMessage(OplogSinkerOffsetCollected(offset: MongoOffset)) => handleOplogSinkerOffsetCollected(offset)
    case OplogSinkerOffsetCollected(offset: MongoOffset) => handleOplogSinkerOffsetCollected(offset)
    case SyncControllerMessage(OplogSinkerCollectOffset) => dispatchOplogSinkerCollectOffset
    case _ => //暂时不做其他处理
  }


  def handleOplogSinkerSendOffset: Unit = {
    log.info(s"start to send offset to recorder,id:$syncTaskId")
    val offset = offsetMap.values.toList match {
      case Nil => null
      case hd :: Nil => hd
      case list => list.reduce { (x: MongoOffset, y: MongoOffset) => x.compare(y, true) }
    }
    log.info(s"handleOplogSinkerSendOffset func get offset:${Option(offset).map(_.formatString).getOrElse("")},id:$syncTaskId")
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
    log.info(s"dispatch oplog sinker collect offset to sinkers,id:$syncTaskId")
    context.children.foreach(ref => ref ! OplogSinkerCollectOffset)
  }

  /**
    * 初始化或Sinker的核心方法
    *
    * @todo 支持动态加载
    * @return actorRef
    */
  private def getOrCreateSinker(key: String): ActorRef = {

    def create: ActorRef = context.actorOf(OplogKeyHdfsSimpleSinker.props(taskManager.asInstanceOf[Mongo2HdfsTaskInfoManager], (System.currentTimeMillis() / 1000).toInt).withDispatcher("akka.sinker-dispatcher"), key)

    context.child(key).getOrElse(create)

  }

  /**
    * 初始化sinkers
    */
  override protected def initSinkers: Unit = {
    val sinkerList = (1 to sinkerNum).map(_ => self).toList
    taskManager.sinkerList = sinkerList //很重要
  }


  override def start = super.start

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


  override def preStart(): Unit = {
    super.preStart()
    log.info("OplogSinkerManager switch 2 offline")
  }
}

object OplogKeyHdfsByNameSinkerManager {
  def props(taskManager: HdfsSinkManagerImp with TaskManager): Props = Props(new OplogKeyHdfsByNameSinkerManager(taskManager))
}

