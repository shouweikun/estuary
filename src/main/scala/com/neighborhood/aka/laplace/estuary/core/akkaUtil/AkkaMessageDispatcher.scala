package com.neighborhood.aka.laplace.estuary.core.akkaUtil

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.akkaUtil.AkkaMessageDispatcher.{addListeners, removeAndListeners, removeListeners}

/**
  * Created by john_liu on 2018/7/27.
  */
class AkkaMessageDispatcher(
                             listeners: List[ActorRef] = List.empty,
                             syncTaskId: String = ""
                           ) extends Actor with ActorLogging {
  private val listenerRefSet: scala.collection.mutable.HashSet[ActorRef] = new scala.collection.mutable.HashSet[ActorRef].++=(listeners)

  override def receive: Receive = {
    case addListeners(refList) => addListeners(refList)
    case removeListeners(refList) => removeListeners(refList)
    case removeAndListeners(removeList, addList) => removeListeners(removeList); addListeners(addList)
    case x => listenerRefSet.foreach(ref => ref ! x)
  }

  /**
    * 增加listener
    *
    * @param listeners
    */
  def addListeners(listeners: List[ActorRef]): Unit = {
    log.info(s"add listeners:${listeners.map(_.toString()).mkString(",")},id:$syncTaskId")
    listenerRefSet.++=(listeners)
    log.info(s"listeners after addition:${listenerRefSet.map(_.toString()).mkString(",")},id:$syncTaskId")
  }

  /**
    * 删除listener
    *
    * @param listeners
    */
  def removeListeners(listeners: List[ActorRef]): Unit = {
    log.info(s"remove listeners:${listeners.map(_.toString()).mkString(",")},id:$syncTaskId")
    listenerRefSet.--=(listeners)
    log.info(s"listeners after removing :${listenerRefSet.map(_.toString()).mkString(",")},id:$syncTaskId")
  }
}

object AkkaMessageDispatcher {
  def props(listeners: List[ActorRef] = List.empty, syncTaskId: String = ""): Props = Props(new AkkaMessageDispatcher(listeners))

  case class addListeners(refList: List[ActorRef])

  case class removeListeners(refList: List[ActorRef])

  case class removeAndListeners(removeList: List[ActorRef], addList: List[ActorRef])

}