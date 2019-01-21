package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait MysqlBinlogInOrderBatcherCommand

object MysqlBinlogInOrderBatcherCommand {

  case object MysqlBinlogInOrderBatcherStart extends MysqlBinlogInOrderBatcherCommand

  case object MysqlBinlogInOrderBatcherCheckHeartbeats extends MysqlBinlogInOrderBatcherCommand
}
