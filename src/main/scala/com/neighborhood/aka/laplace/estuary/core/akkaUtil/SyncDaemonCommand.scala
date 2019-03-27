package com.neighborhood.aka.laplace.estuary.core.akkaUtil

import com.neighborhood.aka.laplace.estuary.core.task.SyncTask

/**
  * Created by john_liu on 2019/1/16.
  *
  * syncDaemon的Command
  *
  * - ExternalStartCommand 启动
  * - ExternalSuspendCommand/ExternalSuspendTimedCommand 挂起/定时挂起
  * - ExternalResumeCommand 唤醒
  * - ExternalRestartCommand 重启
  * - ExternalStopCommand 停止
  * - ExternalGetAllRunningTask 查看所有运行任务
  * - ExternalGetCertainRunningTask 查看特定运行任务
  *
  * @author neighborhood.aka.laplace
  */
sealed trait SyncDaemonCommand

object SyncDaemonCommand {

  case class ExternalStartCommand(syncTask: SyncTask) extends SyncDaemonCommand

  case object ExternalStartCommand extends SyncDaemonCommand

  case class ExternalSuspendCommand(syncTaskId: String) extends SyncDaemonCommand

  case class ExternalResumeCommand(syncTaskId: String) extends SyncDaemonCommand

  case class ExternalSuspendTimedCommand(syncTaskId: String, ts: Long) extends SyncDaemonCommand

  case class ExternalRestartCommand(syncTaskId: String) extends SyncDaemonCommand

  case class ExternalStopCommand(syncTaskId: String) extends SyncDaemonCommand

  case object ExternalStopCommand extends SyncDaemonCommand

  case object ExternalGetAllRunningTask extends SyncDaemonCommand

  case class ExternalGetCertainRunningTask(syncTaskId: String) extends SyncDaemonCommand

}

