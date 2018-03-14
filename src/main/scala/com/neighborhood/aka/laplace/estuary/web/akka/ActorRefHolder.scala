package com.neighborhood.aka.laplace.estuary.web.akka

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.akka.{SyncDaemon, theActorSystem}

/**
  * Created by john_liu on 2018/3/10.
  */
object ActorRefHolder extends theActorSystem {
  //todo 初始化守护Actor
  //todo 保留重要ActorRef
  val syncDaemon = system.actorOf(Props(classOf[SyncDaemon]), "syncDaemon")

  var actorRefMap:Map[String, ActorRef] = Map.empty

  def addNewTaskActorRef(key:String,value:ActorRef): Boolean = {
     if(actorRefMap.get(key).isEmpty){
       actorRefMap = actorRefMap.+(key->value)
       true
     }else{
       false
     }
  }
}
