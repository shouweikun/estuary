package com.neighborhood.aka.laplace.estuary.mysql.akkaUtil

import akka.routing.{RoundRobinRoutingLogic, Routee, RoutingLogic}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.EventType

import scala.collection.immutable

/**
  * Created by john_liu on 2018/4/3.
  *
  * @note https://doc.akka.io/docs/akka/current/routing.html
  *       参考了Akka官方的Router的自定义
  */
class DivideDDLRoundRobinRoutingLogic extends RoutingLogic {
  val roundRobin = RoundRobinRoutingLogic()

  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
    val ddlBatcher = routees.head
    routees(0)
    message match {
      case entry: CanalEntry.Entry if (isConcernedDDL(entry.getHeader.getEventType)) =>
        ddlBatcher
      case _ => roundRobin.select(message, routees.drop(0))
    }

  }

  /**
    * @param eventType binlog事件类型
    *                  目前仅处理Alter一种DDL
    */
  def isConcernedDDL(eventType: CanalEntry.EventType): Boolean = {
    eventType match {
      case EventType.ALTER =>
        true
      case _ => false
    }
  }

}
