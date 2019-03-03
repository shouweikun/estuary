package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.bean.key.OplogKey
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mongo.sink.OplogKeyKafkaSinkManagerImp

/**
  * Created by john_liu on 2019/3/2.
  *
  * @author neighborhood.aka.laplace
  */
final class OplogSinkerManager(
                                override val taskManager: OplogKeyKafkaSinkManagerImp with TaskManager
                              ) extends SourceDataSinkerManagerPrototype[KafkaSinkFunc[OplogKey, String]] {
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
  override lazy val sinkManager: SinkManager[KafkaSinkFunc[OplogKey, String]] = taskManager

  /**
    * sinker的数量
    */
  override val sinkerNum: Int = taskManager.sinkerNum

  /**
    * 在线模式
    *
    * @return
    */
  override protected def online: Receive = ???

  /**
    * 初始化sinkers
    */
  override protected def initSinkers: Unit = {

  }

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

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
