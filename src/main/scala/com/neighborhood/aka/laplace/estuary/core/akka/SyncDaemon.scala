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
      val actorAndReason = startNewTask(prop, name)
      log.info(s"${actorAndReason._2}")
      //保存这个任务的ActorRef
      if (ActorRefHolder.addNewTaskActorRef(name.get, actorAndReason._1))
        log.info(s"actorRef:${name.get} 添加成功") else log.warning(s"actorRef:${name.get} 添加失败")
      actorAndReason._1 ! "start"
    }
    case x => log.warning(s"SyncDeamon unhandled message $x")
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: InvalidActorNameException => Resume
      case _ => Restart
    }
  }

  def startNewTask(prop: Props, name: Option[String]): (ActorRef, String) = {

    name match {
      case Some(x) => {
        if (context.child(x).isDefined) {
          (context.child(x).get, s"该任务id:${name.get}已经存在")
        } else {
          (context.actorOf(prop, x), s"任务id:${name.get}启动成功")
        }
      }
      case None => (null, "不可以空缺任务id!")
    }
  }


}

