package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.adapt

import com.neighborhood.aka.laplace.estuary.core.eventsource.EstuaryEvent

/**
  * Created by john_liu on 2019/1/19.
  */
sealed trait MysqlBinlogInOrderPowerAdapterEvent extends EstuaryEvent

object MysqlBinlogInOrderPowerAdapterEvent {
  case class MysqlBinlogInOrderPowerAdapterCostUpdated(cost: Long) extends MysqlBinlogInOrderPowerAdapterEvent

  case class MysqlBinlogInOrderPowerAdapterFetcherCountUpdated(count: Long) extends MysqlBinlogInOrderPowerAdapterEvent //special case 因为fetcher的计数特性

  case object MysqlBinlogInOrderPowerAdapterControlled extends MysqlBinlogInOrderPowerAdapterEvent

  case object MysqlBinlogInOrderPowerAdapterCostComputed extends MysqlBinlogInOrderPowerAdapterEvent

  case class MysqlBinlogInOrderPowerAdapterFetchDelayed(delay:Long) extends MysqlBinlogInOrderPowerAdapterEvent

}
