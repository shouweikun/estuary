package com.neighborhood.aka.laplace.estuary.mysql.snapshot

import com.neighborhood.aka.laplace.estuary.core.snapshot.SnapshotStateMachine.Mysql2KafkaSnapshotTask

/**
  * Created by john_liu on 2018/10/11.
  */
sealed trait MysqlSnapshotCommand

object MysqlSnapshotCommand {

  case class MysqlSnapshotStartSnapshotTask(task: Mysql2KafkaSnapshotTask) extends MysqlSnapshotCommand

  case object MysqlSnapshotSuspend extends MysqlSnapshotCommand

  case object MysqlSnapshotTerminate extends MysqlSnapshotCommand

  case object MysqlSnapshotRestore extends MysqlSnapshotCommand

  case object MysqlSnapshotCheckRunningStatus extends MysqlSnapshotCommand

}
