package com.neighborhood.aka.laplace.estuary.web.akka

import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.akka.{SyncDaemon, theActorSystem}

import scala.collection.mutable

/**
  * Created by john_liu on 2018/3/10.
  */
object ActorRefHolder extends theActorSystem {
  //todo 初始化守护Actor
  //todo 保留重要ActorRef
  val syncDaemon = system.actorOf(Props(classOf[SyncDaemon]), "syncDaemon")

  val actorRefMap: ConcurrentHashMap[String,ActorRef] = new ConcurrentHashMap[String,ActorRef]()



  def addNewTaskActorRef(key: String, value: ActorRef): Boolean = {
    if (Option(actorRefMap.get(key)).isEmpty) {
      actorRefMap.put(key, value)
      true
    } else {
      false
    }
  }
}
