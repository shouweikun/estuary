package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink

import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait OplogSinkerCommand

object OplogSinkerCommand {
  case object OplogSinkerStart extends OplogSinkerCommand

  case class OplogSinkerGetAbnormal(e: Throwable, offset: Option[BinlogPositionInfo]) extends OplogSinkerCommand

  case object OplogSinkerCheckBatch extends OplogSinkerCommand

}


