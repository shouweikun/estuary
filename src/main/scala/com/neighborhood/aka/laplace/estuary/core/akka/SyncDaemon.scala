package com.neighborhood.aka.laplace.estuary.core.akka

import akka.actor.SupervisorStrategy.{Restart, Resume}
import akka.actor.{Actor, ActorLogging, ActorRef, InvalidActorNameException, OneForOneStrategy, Props}
import com.neighborhood.aka.laplace.estuary.web.akka.ActorRefHolder

/**
  * Created by john_liu on 2018/3/10.
  */
class SyncDaemon extends Actor with ActorLogging {


  override def receive: Receive = {
    case (prop: Props, name: Option[String]) => {
      name.fold(
        log.warning("不可以空缺任务id!")
      )(taskName => {
        val actorAndReason = startNewTask(prop, taskName)
        log.info(s"${actorAndReason._2}")
        actorAndReason._1 ! "start"
      })
    }
    case x => log.warning(s"SyncDeamon unhandled message $x")
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case InvalidActorNameException(_) => Resume
      case _ => Restart
    }
  }

  def startNewTask(prop: Props, name: String): (ActorRef, String) = {
    context.child(name).fold {
      val actorAndReason = (context.actorOf(prop, name), s"任务id:$name 创建成功")
      //保存这个任务的ActorRef
      if (ActorRefHolder.addNewTaskActorRef(name, actorAndReason._1))
        log.info(s"actorRef:${name} 添加成功") else log.warning(s"actorRef:${name} 添加失败")
        actorAndReason
    }(actorRef => (actorRef, s"任务id:$name 已经存在"))
  }


}

