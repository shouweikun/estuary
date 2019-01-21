package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait MysqlBinlogInOrderFetcherEvent
object MysqlBinlogInOrderFetcherEvent {
  case object MysqlBinlogInOrderFetcherStarted extends MysqlBinlogInOrderFetcherEvent
  case object MysqlBinlogInOrderFetcherReStarted extends MysqlBinlogInOrderFetcherEvent
  case object MysqlBinlogInOrderFetcherSuspended  extends MysqlBinlogInOrderFetcherEvent
  case object MysqlBinlogInOrderFetcherBusyChanged  extends MysqlBinlogInOrderFetcherEvent
  case object MysqlBinlogInOrderFetcherFreeChanged  extends MysqlBinlogInOrderFetcherEvent
}
