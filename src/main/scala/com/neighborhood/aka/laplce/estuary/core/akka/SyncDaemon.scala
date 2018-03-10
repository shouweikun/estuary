package com.neighborhood.aka.laplce.estuary.core.akka

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorLogging, ActorRef, AllForOneStrategy, OneForOneStrategy, Props}

/**
  * Created by john_liu on 2018/3/10.
  */
class SyncDaemon extends Actor with ActorLogging {


  override def receive: Receive = {
    case (prop:Props,name:Option[String]) => {
      startNewTask(prop,name) ! "start"
    }
    case x => log.warning(s"SyncDeamon unhandled message $x")
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case _ => Restart
    }
  }

  def startNewTask(prop:Props,name:Option[String]):ActorRef = {
      if(name.isDefined){
        context.actorOf(prop,name.get)
      }else context.actorOf(prop)

  }
}

