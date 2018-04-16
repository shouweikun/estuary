package com.neighborhood.aka.laplace.estuary.mysql.akkaUtil

import akka.actor.ActorSystem
import akka.dispatch.Dispatchers
import akka.routing.{Group, Router}

import scala.collection.immutable


/**
  * Created by john_liu on 2018/4/3.
  * @note https://doc.akka.io/docs/akka/current/routing.html
  * 参考了Akka官方的Router的自定义
  */
final case class DivideDDLRoundRobinRoutingGroup(routeePaths:immutable.Iterable[String]) extends Group {

  override def paths(system: ActorSystem): immutable.Iterable[String] = routeePaths

  override def createRouter(system: ActorSystem): Router = new Router(new DivideDDLRoundRobinRoutingLogic)

  override def routerDispatcher: String = Dispatchers.DefaultDispatcherId

}
