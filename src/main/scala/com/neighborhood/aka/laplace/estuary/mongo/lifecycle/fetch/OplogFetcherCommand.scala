package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait MysqlBinlogInOrderFetcherCommand

object MysqlBinlogInOrderFetcherCommand {

  case object MysqlBinlogInOrderFetcherStart extends MysqlBinlogInOrderFetcherCommand

  case object MysqlBinlogInOrderFetcherRestart extends MysqlBinlogInOrderFetcherCommand

  case object MysqlBinlogInOrderFetcherSuspend extends MysqlBinlogInOrderFetcherCommand

  case object MysqlBinlogInOrderFetcherBusy extends MysqlBinlogInOrderFetcherCommand

  case object MysqlBinlogInOrderFetcherFree extends MysqlBinlogInOrderFetcherCommand

  case object MysqlBinlogInOrderResume extends MysqlBinlogInOrderFetcherCommand

  case object MysqlBinlogInOrderFetcherPrefetch extends MysqlBinlogInOrderFetcherCommand

  case object MysqlBinlogInOrderFetcherFetch extends MysqlBinlogInOrderFetcherCommand

  case object MysqlBinlogInOrderFetcherNonBlockingFetch extends MysqlBinlogInOrderFetcherCommand

  case class MysqlBinlogInOrderFetcherUpdateDelay(delay: Long) extends MysqlBinlogInOrderFetcherCommand

}
