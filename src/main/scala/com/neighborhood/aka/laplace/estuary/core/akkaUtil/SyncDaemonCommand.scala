package com.neighborhood.aka.laplace.estuary.core.akkaUtil

import com.neighborhood.aka.laplace.estuary.core.task.SyncTask

/**
  * Created by john_liu on 2019/1/16.
  */
sealed trait SyncDaemonCommand

object SyncDaemonCommand {

  case class ExternalStartCommand(syncTask: SyncTask) extends SyncDaemonCommand

  case object ExternalStartCommand


  case class ExternalRestartCommand(syncTaskId: String) extends SyncDaemonCommand

  case class ExternalStopCommand(syncTaskId: String) extends SyncDaemonCommand

  case object ExternalStopCommand extends SyncDaemonCommand

  case object ExternalGetAllRunningTask extends SyncDaemonCommand

  case class ExternalGetCertainRunningTask(syncTaskId:String) extends SyncDaemonCommand

}

