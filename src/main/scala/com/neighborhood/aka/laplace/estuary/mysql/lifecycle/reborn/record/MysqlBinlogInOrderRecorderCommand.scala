package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.record

import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait MysqlBinlogInOrderRecorderCommand

object MysqlBinlogInOrderRecorderCommand {

  case object MysqlBinlogInOrderRecorderSavePosition extends MysqlBinlogInOrderRecorderCommand

  case object MysqlBinlogInOrderRecorderSaveLatestPosition extends MysqlBinlogInOrderRecorderCommand

  case class MysqlBinlogInOrderRecorderEnsurePosition(binlogPositionInfo:BinlogPositionInfo) extends MysqlBinlogInOrderRecorderCommand

}
