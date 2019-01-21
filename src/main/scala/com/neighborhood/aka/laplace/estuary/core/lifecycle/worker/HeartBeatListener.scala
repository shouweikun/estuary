package com.neighborhood.aka.laplace.estuary.core.lifecycle.worker

/**
  * Created by john_liu on 2018/2/6.
  *
  */
trait HeartBeatListener extends worker {
  implicit val workerType = WorkerType.Listener

  /**
    * 监听心跳
    */
  protected def listenHeartBeats: Unit
}
