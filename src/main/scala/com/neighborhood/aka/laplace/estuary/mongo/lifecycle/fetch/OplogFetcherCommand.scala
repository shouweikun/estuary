package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait OplogFetcherCommand

object OplogFetcherCommand {
  case object MysqlBinlogInOrderFetcherStart extends OplogFetcherCommand

  case object MysqlBinlogInOrderFetcherRestart extends OplogFetcherCommand

  case object MysqlBinlogInOrderFetcherSuspend extends OplogFetcherCommand

  case object MysqlBinlogInOrderFetcherBusy extends OplogFetcherCommand

  case object MysqlBinlogInOrderFetcherFree extends OplogFetcherCommand

  case object MysqlBinlogInOrderResume extends OplogFetcherCommand

  case object MysqlBinlogInOrderFetcherPrefetch extends OplogFetcherCommand

  case object MysqlBinlogInOrderFetcherFetch extends OplogFetcherCommand

  case object MysqlBinlogInOrderFetcherNonBlockingFetch extends OplogFetcherCommand

  case class MysqlBinlogInOrderFetcherUpdateDelay(delay: Long) extends OplogFetcherCommand

}