package com.neighborhood.aka.laplace.estuary.actor

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActors, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
  * Created by john_liu on 2018/2/9.
  */
class MysqlBinlogFetcher extends TestKit(ActorSystem("MySpec")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {
  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "An Echo actor" must {

    "send back messages unchanged" in {
     val questReceiver = TestProbe()

      val binlogFetcher
      = system.actorOf(Props(classOf[MysqlBinlogFetcher]))
      binlogFetcher

    }

  }
}
