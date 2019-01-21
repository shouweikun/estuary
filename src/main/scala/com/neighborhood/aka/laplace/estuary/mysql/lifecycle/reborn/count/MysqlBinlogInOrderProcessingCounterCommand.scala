package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.count

/**
  * Created by john_liu on 2019/1/11.
  */
sealed trait MysqlBinlogInOrderProcessingCounterCommand

object MysqlBinlogInOrderProcessingCounterCommand {

  case class MysqlBinlogInOrderProcessingCounterUpdateCount(count: Long) extends MysqlBinlogInOrderProcessingCounterCommand

  case object MysqlBinlogInOrderProcessingCounterComputeCount extends MysqlBinlogInOrderProcessingCounterCommand

}