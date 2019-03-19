package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.hbase

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.bean.support.HBasePut
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.sink.hbase.{HBaseSinkFunc, HBaseSinkManager}
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.OplogSinkerCommand.OplogSinkerStart
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset

/**
  * Created by john_liu on 2019/3/15.
  *
  * @note send by tableName
  */
class OplogKeyHBaseByNameSinkerManager(
                                        override val taskManager: HBaseSinkManager with TaskManager
                                      ) extends SourceDataSinkerManagerPrototype[HBaseSinkFunc] {
  /**
    * sinker名称 用于动态加载
    */
  override val sinkerName: String = "sinkerName"


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


  override def receive: Receive = {
    case SyncControllerMessage(OplogSinkerStart) => start
  }

  /**
    * 在线模式
    *
    * @return
    */
  override protected def online: Receive = {
    //case _ => SyncControllerMessage()
    case x: HBasePut[MongoOffset] => getOrCreateSinker(x.tableName) ! x
    case BatcherMessage(x: HBasePut[MongoOffset]) => getOrCreateSinker(x.tableName) ! x
    case _ => //暂时不做其他处理

  }

  /**
    * 初始化或Sinker的核心方法
    *
    * @todo 支持动态加载
    * @return actorRef
    */
  private def getOrCreateSinker(key: String): ActorRef = {

    def create: ActorRef = context.actorOf(OplogKeyHBaseRingBufferSinker.props(taskManager, (System.currentTimeMillis() / 1000).toInt).withDispatcher("akka.sinker-dispatcher"), key)

    context.child(key).getOrElse(create)

  }

  /**
    * 初始化sinkers
    *
    *
    * 因为想用tableName作区分，所以其实initSinker阶段没有做事情
    */
  override protected def initSinkers: Unit = {
    val sinkerList = (1 to sinkerNum).map(_ => self).toList
    taskManager.sinkerList = sinkerList //很重要
  }

  /**
    * 错位次数阈值
    */
  override def errorCountThreshold: Int = ???

  /**
    * 错位次数
    */
  override var errorCount: Int = _

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???

}

object OplogKeyHBaseByNameSinkerManager {
  lazy val name = OplogKeyHBaseByNameSinkerManager.getClass.getName.stripSuffix("$")
  def props(taskManager: HBaseSinkManager with TaskManager): Props = Props(new OplogKeyHBaseByNameSinkerManager(taskManager))
}