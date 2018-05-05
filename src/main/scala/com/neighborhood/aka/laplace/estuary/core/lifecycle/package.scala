package com.neighborhood.aka.laplace.estuary.core

/**
  * Created by john_liu on 2018/2/9.
  */
package object lifecycle {

  trait WorkerMessage {
    val msg: String
    val data: Option[Any] = None
  }

  case class SyncControllerMessage(msg: Any) extends WorkerMessage

  case class ListenerMessage(msg: Any) extends WorkerMessage

  case class SinkerMessage(msg: Any) extends WorkerMessage

  case class FetcherMessage(msg: Any) extends WorkerMessage

  case class BatcherMessage(msg: Any) extends WorkerMessage

  case class RecorderMessage(msg: Any) extends WorkerMessage

}
