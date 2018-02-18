package com.neighborhood.aka.laplace.estuary.actor

import com.neighborhood.aka.laplce.estuary.mysql.LogPositionHandler
import org.scalatest._

/**
  * Created by john_liu on 2018/2/18.
  */
class LogPositionHandlerTest extends FlatSpec with BeforeAndAfterAll with Matchers {

  val logPositionHandler = TestContext.mysql2KafkaTaskInfoManager.logPositionHandler
  val mysqlConnection = TestContext.mysql2KafkaTaskInfoManager.mysqlConnection

  override protected def beforeAll(): Unit = {
    mysqlConnection.connect()
  }

  "findEndPositionTest" should "find the last Position" in {
    val entryPosition = logPositionHandler.findEndPosition(mysqlConnection)
    assert(Option(entryPosition).isDefined)
    assert(entryPosition.getPosition ||)
  }


}
