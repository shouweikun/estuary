package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch

import com.neighborhood.aka.laplace.estuary.core.eventsource.EstuaryEvent

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait MysqlBinlogInOrderBatcherEvent extends EstuaryEvent

object MysqlBinlogInOrderBatcherEvent {

  case object MysqlBinlogInOrderBatcherStarted extends MysqlBinlogInOrderBatcherEvent

  case object MysqlBinlogInOrderBatcherHeartbeatsChecked extends MysqlBinlogInOrderBatcherEvent

}
