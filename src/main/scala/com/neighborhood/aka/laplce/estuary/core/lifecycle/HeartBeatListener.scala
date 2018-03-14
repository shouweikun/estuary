package com.neighborhood.aka.laplce.estuary.core.lifecycle

/**
  * Created by john_liu on 2018/2/6.
  */
trait HeartBeatListener extends worker {
  implicit val workerType = WorkerType.Listener

  def listenHeartBeats: Unit
}
