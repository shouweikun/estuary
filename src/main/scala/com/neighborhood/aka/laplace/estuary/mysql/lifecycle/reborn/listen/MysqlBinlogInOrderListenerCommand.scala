package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.listen

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait MysqlBinlogInOrderListenerCommand

object MysqlBinlogInOrderListenerCommand {

  case object MysqlBinlogInOrderListenerStart extends MysqlBinlogInOrderListenerCommand

  case object MysqlBinlogInOrderListenerListen extends MysqlBinlogInOrderListenerCommand

  case object MysqlBinlogInOrderListenerStop extends MysqlBinlogInOrderListenerCommand

}
