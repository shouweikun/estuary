package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{FetcherMessage, SnapshotJudgerMessage}
import com.neighborhood.aka.laplace.estuary.core.snapshot.SnapshotStateMachine.{Mysql2KafkaSnapshotTask, Mysql2KafkaSnapshotTaskDataSendFinish}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch.MysqlBinlogInOrderFetcherCommand.{MysqlBinlogInOrderFetcherStart, MysqlBinlogInOrderFetcherSuspend}
import com.neighborhood.aka.laplace.estuary.mysql.snapshot.MysqlSnapshotStateMachine

import scala.concurrent.duration._

/**
  * Created by john_liu on 2018/7/27.
  * @todo 重写
  */
class MysqlBinlogInOrderSnapshotJudger(
                                        private val mysqlSnapshotStateMachine: MysqlSnapshotStateMachine,
                                        private val syncTaskId: String,
                                        private val downStream: ActorRef
                                      ) extends Actor with ActorLogging {

  implicit val ec = context.dispatcher

  /**
    * 1.entry => 检查entry
    * 2.FetcherMessage(MysqlBinlogInOrderFetcherStart) => start
    * 3.SnapshotJudgerMessage("check") => 检查entry
    * 4.FetcherMessage("suspend") => suspend
    * 5.FetcherMessage(task: SnapshotTask)=> 开启一个快照任务
    *
    * @return
    */
  override def receive: Receive = {
    case FetcherMessage(MysqlBinlogInOrderFetcherStart) => start
    case FetcherMessage(MysqlBinlogInOrderFetcherSuspend) => suspend
    case entry: CanalEntry.Entry => judgeSnapshotStatusByEntryAndHandle(entry)
    case SnapshotJudgerMessage("check") => judgeSnapshotStatusAndSendJudgeMessage()
    case FetcherMessage(task: Mysql2KafkaSnapshotTask) => startSnapshotTask(task)
  }

  /**
    * 挂起
    * 1.FetcherMessage("resume")=>finishSnapshotTask
    *
    * @return
    */
  def suspend: Receive = {
    case FetcherMessage("resume") => finishSnapshotTask
    case _ =>
  }

  /**
    * 1.发送检查命令
    * 2.检查当前状态机状态并处理
    */
  private def start: Unit = {
    log.info(s"MysqlBinlogInOrderSnapshotJudger switch 2 start,id:$syncTaskId")
    checkSnapshotStatusAndChangeStatus
  }

  /**
    * 切换为挂起
    */
  private def switch2Suspend = {
    log.info(s"switch 2 suspend,id:$syncTaskId")
    context.become(suspend, true)
  }

  /**
    * 开始一个新的快照任务
    *
    * @param task
    */
  private def startSnapshotTask(task: Mysql2KafkaSnapshotTask): Unit = {
    log.info(s"start snapshot task:$task,id:$syncTaskId")
    mysqlSnapshotStateMachine.startSnapshotTask(task)
    checkSnapshotStatusAndChangeStatus
  }

  private def finishSnapshotTask: Unit = {
    log.info(s"start snapshot task:${mysqlSnapshotStateMachine.buildSnapshotTaskInfo},id:$syncTaskId")
    mysqlSnapshotStateMachine.onTerminatingTask
    context.become(receive, true)
    sender ! FetcherMessage("resumed")
  }

  /**
    * 检测状态并进行对应操作
    * 1.无任务
    * 2.收集数据
    * 3.挂起  =>
    */
  private def checkSnapshotStatusAndChangeStatus: Unit = {
    lazy val hasRunningTask = mysqlSnapshotStateMachine.hasRunningTask
    lazy val isSuspend = mysqlSnapshotStateMachine.isSuspend
    log.info(s"start check Snapshot Status And Change Status,id:$syncTaskId")
    (hasRunningTask, isSuspend) match {
      case (false, _) => log.info(s"no running snapshot task,id:$syncTaskId")
      case (true, false) => log.info(s"has running snapshot task:${mysqlSnapshotStateMachine.buildSnapshotTaskInfo},id:$syncTaskId")
      case (_, true) => log.info(s"has running snapshot task:${mysqlSnapshotStateMachine.buildSnapshotTaskInfo} and is on suspend ,id:$syncTaskId");

    }
    if (isSuspend) {
      switch2Suspend
      context.parent ! FetcherMessage(Mysql2KafkaSnapshotTaskDataSendFinish(mysqlSnapshotStateMachine.buildSnapshotTaskInfo))
      context.parent ! FetcherMessage("suspend")

    } else {
      sendJudgeSnapshotStatusCommand()
    }
  }

  private def judgeSnapshotStatusByEntryAndHandle(entry: CanalEntry.Entry): Unit = {
    if (judgeEntry4SnapshotStatus(Option(entry))) handleDataQualified else downStream ! entry
  }

  private def judgeSnapshotStatusAndSendJudgeMessage(delay: Long = 30000): Unit = {
    judgeEntry4SnapshotStatus(None) match {
      case false => sendJudgeSnapshotStatusCommand(delay)
      case true => handleDataQualified
    }

  }

  private def judgeEntry4SnapshotStatus(entry: Option[CanalEntry.Entry] = None): Boolean = {
    mysqlSnapshotStateMachine.judgeSnapshotData(entry)
  }

  private def handleDataQualified: Unit = {
    mysqlSnapshotStateMachine.onSuspend
    context.become(suspend, true)
    sendSnapshotTaskEndMessage()
  }

  private def sendJudgeSnapshotStatusCommand(delay: Long = 0): Unit = {
    lazy val message = SnapshotJudgerMessage("check")
    log.debug(s"prepare sending next JudgeSnapshotStatusCommand,id:$syncTaskId")
    delay match {
      case x if (x > 0) => context.system.scheduler.scheduleOnce(delay millisecond, self, message)
      case _ => self ! message
    }
  }

  private def sendSnapshotTaskEndMessage(downStream: ActorRef = downStream, parent: ActorRef = context.parent): Unit = {

    lazy val message = FetcherMessage(Mysql2KafkaSnapshotTaskDataSendFinish(this.mysqlSnapshotStateMachine.buildSnapshotTaskInfo))
    log.info(s"start send snapshot task end Message,$message")
    //    downStream ! message
    parent ! message
  }

  override def preStart(): Unit = {
    log.info(s"snapshotJudger processes preStart,id:$syncTaskId")
  }
}

object MysqlBinlogInOrderSnapshotJudger {
  def props(
             mysqlSnapshotStateMachine: MysqlSnapshotStateMachine,
             syncTaskId: String, downStream: ActorRef
           ): Props = Props(new MysqlBinlogInOrderSnapshotJudger(
    mysqlSnapshotStateMachine, syncTaskId, downStream))
}