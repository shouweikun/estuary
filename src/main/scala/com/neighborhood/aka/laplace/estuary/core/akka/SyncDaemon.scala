package com.neighborhood.aka.laplace.estuary.core.akka

import akka.actor.SupervisorStrategy.{Escalate, Restart, Resume}
import akka.actor.{Actor, ActorLogging, ActorRef, AllForOneStrategy, InvalidActorNameException, OneForOneStrategy, Props}
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
        //保存这个任务的ActorRef
        if (ActorRefHolder.addNewTaskActorRef(name.get, actorAndReason._1))
          log.info(s"actorRef:${name.get} 添加成功") else log.warning(s"actorRef:${name.get} 添加失败")
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
      context.child(name).fold(
        (context.child(name).get, s"该任务id:$name 已经存在")
      )(actorRef => (actorRef,s"任务id:$name 启动成功"))
  }


}

