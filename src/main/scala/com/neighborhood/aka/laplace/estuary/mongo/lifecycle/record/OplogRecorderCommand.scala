package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.record

import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo

/**
  * Created by john_liu on 2019/3/6.
  */

sealed trait  OplogRecorderCommand
object OplogRecorderCommand {
  case object OplogRecorderSavePosition extends OplogRecorderCommand

  case object OplogRecorderSaveLatestPosition extends OplogRecorderCommand

  case class OplogRecorderEnsurePosition(binlogPositionInfo:BinlogPositionInfo) extends OplogRecorderCommand

}
