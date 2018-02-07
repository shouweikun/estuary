package com.neighborhood.aka.laplce.estuary.mysql

/**
  * Created by john_liu on 2018/2/3.
  */
package object actors {
  case class SyncControllerMessage(msg:String)
  case class ListenerMessage(msg:String)
  case class SinkerMessage(msg:String)
  case class FetcherMessage(msg:String)
  case class BatcherMessage(msg:String)
}
