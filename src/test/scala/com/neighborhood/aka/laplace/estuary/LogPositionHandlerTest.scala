package com.neighborhood.aka.laplace.estuary

import org.scalatest._

/**
  * Created by john_liu on 2018/2/18.
  */
class LogPositionHandlerTest extends FlatSpec with BeforeAndAfterAll with Matchers {

// // val logPositionHandler = TestContext.mysql2KafkaTaskInfoManager.logPositionHandler
//  val mysqlConnection = TestContext.mysql2KafkaTaskInfoManager.mysqlConnection
//  val canalEventParser: MysqlEventParser = TestContext.dummyMysqlEventParser.newInstance().asInstanceOf[MysqlEventParser]
//
//  override protected def beforeAll(): Unit = {
//    mysqlConnection.connect()
//  }
//
//  "findEndPositionTest" should "find the last Position" in {
//    val entryPosition = logPositionHandler.findEndPosition(mysqlConnection)
//    assert(Option(entryPosition).isDefined)
//    println(entryPosition.getJournalName)
//    println(entryPosition.getPosition)
//  }
//
//  "findAsPerTimestampInSpecificLogFileTest" should "find the nearest binlogFile " in {
//    //todo scala 反射
//    //初始化时间戳
//    val timeStamp = System.currentTimeMillis()
//    val entryPosition = logPositionHandler
//  }
//
//  "findStartPositionTest" should "find the start position" in {
//    val entryPosition = logPositionHandler.findStartPosition(mysqlConnection)(false)
//    assert(Option(entryPosition).isDefined)
//  }
//
//  "findStartPositionInternal" should "find the correct binlogFile" in {
//    val entryPosition = logPositionHandler.findStartPositionInternal(mysqlConnection)(false)
//    assert(Option(entryPosition).isDefined)
//  }
//
//  "findTransactionBeginPositionTest" should "find the transcationed begin position" in {
//    val entryPosition = logPositionHandler.findEndPosition(mysqlConnection)
//    //原生的canal方法
//    val canalMethod =  TestContext.dummyMysqlEventParser.getMethod("findTransactionBeginPosition",classOf[ErosaConnection],classOf[EntryPosition])
//    canalMethod.setAccessible(true)
//    val canalEntryPosition = canalMethod.invoke(canalEventParser, mysqlConnection, entryPosition)
//
//    val position = logPositionHandler.findTransactionBeginPosition(mysqlConnection, entryPosition)
//
//
//    assert(Option(position).isDefined)
//  }
}
