package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait MysqlInOrderSinkerEvent
object MysqlInOrderSinkerEvent {
  case object MysqlInOrderSinkerStarted extends MysqlInOrderSinkerEvent

  case object MysqlInOrderSinkerOffsetSaved extends MysqlInOrderSinkerEvent

  case object MysqlInOrderSinkerGetAbnormal extends MysqlInOrderSinkerEvent
}
