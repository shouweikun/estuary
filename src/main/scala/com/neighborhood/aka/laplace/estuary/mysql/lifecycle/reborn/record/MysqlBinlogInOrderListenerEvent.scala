package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.record

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait MysqlBinlogInOrderPositionRecorderEvent

object MysqlBinlogInOrderPositionRecorderEvent {

  case object MysqlBinlogInOrderListenerStarted extends MysqlBinlogInOrderPositionRecorderEvent

  case object MysqlBinlogInOrderListenerListened extends MysqlBinlogInOrderPositionRecorderEvent

  case object MysqlBinlogInOrderListenerStopped extends MysqlBinlogInOrderPositionRecorderEvent

}
