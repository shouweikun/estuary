package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.record

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait MysqlBinlogInOrderRecorderCommand

object MysqlBinlogInOrderRecorderCommand {

  case object MysqlBinlogInOrderRecorderSavePosition extends MysqlBinlogInOrderRecorderCommand

}
