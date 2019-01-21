package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink

import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait MysqlBinlogInOrderSinkerCommand

object MysqlBinlogInOrderSinkerCommand {

  case object MysqlInOrderSinkerStart extends MysqlBinlogInOrderSinkerCommand

  case class MysqlInOrderSinkerGetAbnormal(e: Throwable, offset: Option[BinlogPositionInfo]) extends MysqlBinlogInOrderSinkerCommand

  case object MysqlInOrderSinkerCheckBatch extends MysqlBinlogInOrderSinkerCommand

}
