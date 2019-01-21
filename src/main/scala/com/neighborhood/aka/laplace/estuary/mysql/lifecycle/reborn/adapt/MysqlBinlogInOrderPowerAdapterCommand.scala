package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.adapt

import com.neighborhood.aka.laplace.estuary.core.eventsource.EstuaryCommand

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait MysqlBinlogInOrderPowerAdapterCommand extends EstuaryCommand

object MysqlBinlogInOrderPowerAdapterCommand {

  case class MysqlBinlogInOrderPowerAdapterUpdateCost(cost: Long) extends MysqlBinlogInOrderPowerAdapterCommand

  case class MysqlBinlogInOrderPowerAdapterUpdateFetcherCount(count: Long) extends MysqlBinlogInOrderPowerAdapterCommand //special case 因为fetcher的计数特性

  case object MysqlBinlogInOrderPowerAdapterControl extends MysqlBinlogInOrderPowerAdapterCommand

  case object MysqlBinlogInOrderPowerAdapterComputeCost extends MysqlBinlogInOrderPowerAdapterCommand

  case class MysqlBinlogInOrderPowerAdapterDelayFetch(delay:Long) extends MysqlBinlogInOrderPowerAdapterCommand

}

