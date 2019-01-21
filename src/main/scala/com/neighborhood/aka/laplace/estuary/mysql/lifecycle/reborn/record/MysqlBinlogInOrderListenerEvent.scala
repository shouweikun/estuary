package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.record

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait MysqlBinlogInOrderListenerEvent
object MysqlBinlogInOrderListenerEvent {
  case object MysqlBinlogInOrderListenerStarted extends  MysqlBinlogInOrderListenerEvent
  case object MysqlBinlogInOrderListenerListened extends  MysqlBinlogInOrderListenerEvent
  case object MysqlBinlogInOrderListenerStopped extends  MysqlBinlogInOrderListenerEvent
}
