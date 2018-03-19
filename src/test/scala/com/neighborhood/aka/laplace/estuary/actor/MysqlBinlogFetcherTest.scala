package com.neighborhood.aka.laplace.estuary.actor

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestActors, TestKit, TestProbe}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.MysqlBinlogFetcher
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
  * Created by john_liu on 2018/2/9.
  */
class MysqlBinlogFetcherTest extends TestKit(ActorSystem("MySpec")) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {
//  val taskManager = TestContext.mysql2KafkaTaskInfoManager
//  val questReceiver = TestProbe().ref
//  val actor = TestActorRef.apply(MysqlBinlogFetcher.props(taskManager, questReceiver))
//  actor.underlying
//
//  override def afterAll {
//    TestKit.shutdownActorSystem(system)
//  }

  "An MysqlBinlogFetcher Actor" must {

  }
  //  "An Echo actor" must {
  //
  //    "send back messages unchanged" in {
  //     val questReceiver = TestProbe()
  //
  //      val binlogFetcher
  //      = system.actorOf(Props(classOf[MysqlBinlogFetcher]))
  //      binlogFetcher
  //
  //    }
  //
  //  }
}
