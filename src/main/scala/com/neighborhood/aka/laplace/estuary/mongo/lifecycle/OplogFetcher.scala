package com.neighborhood.aka.laplace.estuary.mongo.lifecycle

import akka.actor.{Actor, ActorLogging}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.SourceDataFetcher
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset
import com.neighborhood.aka.laplace.estuary.mongo.task.Mongo2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/4/26.
  *
  * 1.首先寻找开始位点
  * 2.开始拉取数据
  */
class OplogFetcher(
                    mongo2KafkaTaskInfoManager: Mongo2KafkaTaskInfoManager
                  )
  extends SourceDataFetcher with Actor with ActorLogging {
  /**
    * 寻址处理器
    */
  val logPositionHandler = mongo2KafkaTaskInfoManager.mongoOffsetHandler
  /**
    * 是否记录耗时
    */
  val isCosting = mongo2KafkaTaskInfoManager.taskInfoBean.isCosting
  /**
    * 是否计数
    */
  val isCounting = mongo2KafkaTaskInfoManager.taskInfoBean.isCounting
  /**
    * 暂存的entryPosition
    */
  var entryPosition: Option[MongoOffset] = None

  override def receive: Receive = {
    case "" =>
  }


  def online: Receive = {
    case _ =>
  }


  /**
    * 错位次数阈值
    */
  override var errorCountThreshold: Int = _
  /**
    * 错位次数
    */
  override var errorCount: Int = _

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???
}
