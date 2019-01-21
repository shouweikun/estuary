package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.control

/**
  * Created by john_liu on 2019/1/13.
  */
sealed trait MysqlBinlogInOrderControllerCommand

object MysqlBinlogInOrderControllerCommand {

  case object MysqlBinlogInOrderControllerRestart extends MysqlBinlogInOrderControllerCommand

  case object MysqlBinlogInOrderControllerStart extends MysqlBinlogInOrderControllerCommand

  case object MysqlBinlogInOrderControllerStopAndRestart extends MysqlBinlogInOrderControllerCommand //针对 online时的人为重启

  case object MysqlBinlogInOrderControllerCheckRunningInfo extends MysqlBinlogInOrderControllerCommand

}
