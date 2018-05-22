package com.neighborhood.aka.laplace.estuary.mongo.lifecycle

import akka.actor.{Actor, ActorLogging}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.HeartBeatListener

/**
  * Created by john_liu on 2018/5/4.
  */
class MongoConnectionListener(

                             ) extends HeartBeatListener with Actor with ActorLogging {
  /**
    * 监听心跳
    */
  override def listenHeartBeats: Unit = ???

  override def receive: Receive = ???

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
