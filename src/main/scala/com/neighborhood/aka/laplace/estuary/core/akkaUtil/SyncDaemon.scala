package com.neighborhood.aka.laplace.estuary.core.akkaUtil

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor.{Actor, ActorLogging, ActorRef, InvalidActorNameException, OneForOneStrategy, Props}
import com.neighborhood.aka.laplace.estuary.bean.exception.other.WorkerInitialFailureException
import com.neighborhood.aka.laplace.estuary.core.akkaUtil.SyncDaemonCommand._
import com.neighborhood.aka.laplace.estuary.core.task.{Mongo2HBaseSyncTask, Mongo2KafkaSyncTask, Mysql2MysqlSyncTask}

import scala.util.Try

/**
  * Created by john_liu on 2018/3/10.
  * 同步任务的守护Actor
  *
  * 对同步任务级别的生命周期管理均通过这个Actor访问
  *
  * @author neighborhood.aka.laplace
  */
final class SyncDaemon extends Actor with ActorLogging {


  override def receive: Receive = {

    case ExternalStartCommand(task) => task match {
      case Mysql2MysqlSyncTask(props, name) => startNewTask(props, name)
      case Mongo2KafkaSyncTask(props, name) => startNewTask(props, name)
      case Mongo2HBaseSyncTask(props, name) => startNewTask(props, name)
      case x => log.error(s"$x is ${x.taskType} which is not supported currently")
    }
    case ExternalRestartCommand(syncTaskId) => restartTask(syncTaskId)
    case ExternalStopCommand(syncTaskId) => stopTask(syncTaskId)
    case ExternalSuspendTimedCommand(syncTaskId, ts) => suspendTask(syncTaskId, ts)
    case ExternalGetAllRunningTask => sender ! getAllRunningTask
    case ExternalGetCertainRunningTask(syncTaskId) => sender ! getCertainSyncTaskActorRef(syncTaskId)
    case x => log.warning(s"SyncDeamon unhandled message $x")
  }

  private def getCertainSyncTaskActorRef(name: String): Option[ActorRef] = context.child(name)

  /**
    * 获取全部运行列表
    *
    * @return 全部运行SyncTask的SyncTaskId列表
    */
  private def getAllRunningTask: Iterable[String] = {
    context.children.map(_.path.name)
  }

  /**
    * 停止一个同步任务
    *
    * @param name syncTaskId
    */
  private def stopTask(name: String): Unit = {
    Option(name).flatMap(context.child(_)).fold {
      log.warning(s"does not exist task called $name,no need to stop it")
    } { ref => Try(context.stop(ref)) }
  }

  /**
    * 挂起一个同步任务
    *
    * @param name syncTaskId
    * @param ts   时间戳
    */
  private def suspendTask(name: String, ts: Long = -1): Unit = {
    lazy val command = if (ts == -1) ExternalSuspendCommand(name) else ExternalSuspendTimedCommand(name, ts)
    Option(name).flatMap(getCertainSyncTaskActorRef(_)).fold {
      log.warning(s"does not exist task called $name,no need to suspend it")
    } { ref => ref ! command }
  }

  /**
    * 重启一个同步任务
    *
    * @param name syncTaskId
    */
  private def restartTask(name: String): Unit = {
    Option(name).
      flatMap(getCertainSyncTaskActorRef(_)).fold {
      log.warning(s"does not exist task called $name,no need to restart it")
    } { ref => ref ! ExternalRestartCommand(name) }
  }

  /**
    * @param prop Actor的prop
    * @param name Actor的名字
    * @return （ActorRef, String） 初始化好的Actor的Ref和原因
    *         新建一个同步任务
    */
  private def startNewTask(prop: Props, name: String): (ActorRef, String) = {
    context.child(name).fold {
      val (actor, result) = (context.actorOf(prop, name), s"syncTaskId:$name create success")
      actor ! ExternalStartCommand //发送启动命令
      log.info(s"start task,id:$name,time:${System.currentTimeMillis}")
      (actor, result)
    } {
      actorRef =>
        log.warning(s"syncTaskId:$name has already exists"); (actorRef, s"syncTaskId:$name has already exists")
    }
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case InvalidActorNameException(_) => Stop
      case _: WorkerInitialFailureException => Stop //出现这个异常，说明启动信息和实际加载的类不匹配，应该被停止
      case _ => Restart
    }
  }
}

object SyncDaemon {

  def props = Props(new SyncDaemon)
}
