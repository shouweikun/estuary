package com.neighborhood.aka.laplce.estuary.mysql

/**
  * Created by john_liu on 2018/2/3.
  */
package object actors {
  trait WorkerMessage
  case class SyncControllerMessage(msg:String) extends WorkerMessage
  case class ListenerMessage(msg:String)extends WorkerMessage
  case class SinkerMessage(msg:String)extends WorkerMessage
  case class FetcherMessage(msg:String)extends WorkerMessage
  case class BatcherMessage(msg:String)extends WorkerMessage
}
