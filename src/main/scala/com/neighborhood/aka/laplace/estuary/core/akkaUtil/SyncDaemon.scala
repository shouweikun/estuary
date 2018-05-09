package com.neighborhood.aka.laplace.estuary.core.akkaUtil

import akka.actor.SupervisorStrategy.{Restart, Resume}
import akka.actor.{Actor, ActorLogging, ActorRef, InvalidActorNameException, OneForOneStrategy, Props}
import com.neighborhood.aka.laplace.estuary.core.task.SyncTask
import com.neighborhood.aka.laplace.estuary.web.akkaUtil.ActorRefHolder

/**
  * Created by john_liu on 2018/3/10.
  * 同步任务的守护Actor
  */
class SyncDaemon extends Actor with ActorLogging {


  override def receive: Receive = {

    case task: SyncTask => {
      val name = task.name
      val prop = task.prop
      name.fold(
        log.error(s"不可以空缺任务id!,${task}提交失败")
      )(taskName => {
        val actorAndReason = startNewTask(prop, taskName)
        log.info(s"${actorAndReason._2},taskType:${task.taskType}")
      })
    }
    case x => log.warning(s"SyncDeamon unhandled message $x")
  }

  /**
    * @param prop Actor的prop
    * @param name Actor的名字
    * @return （ActorRef, String） 初始化好的Actor的Ref和原因
    *         新建一个同步任务
    */
  def startNewTask(prop: Props, name: String): (ActorRef, String) = {
    context.child(name).fold {
      val actorAndReason = (context.actorOf(prop, name), s"任务id:$name 创建成功")
      //保存这个任务的ActorRef
      if (ActorRefHolder.addNewTaskActorRef(name, actorAndReason._1))
        log.info(s"actorRef:${name} 添加成功") else log.error(s"actorRef:${name} 添加失败")
      actorAndReason._1 ! "start"
      log.info(s"start task,id:$name,time:${System.currentTimeMillis}")
      actorAndReason
    } { actorRef => log.warning(s"任务id:$name 已经存在"); (actorRef, s"任务id:$name 已经存在") }
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case InvalidActorNameException(_) => Resume
      case _ => Restart
    }
  }

}

