package com.neighborhood.aka.laplace.estuary

import com.neighborhood.aka.laplace.estuary.mysql.CanalEntryJsonHelper
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by john_liu on 2018/4/13.
  */
class CanalEntryJsonHelperTest extends FlatSpec with BeforeAndAfterAll with Matchers {
  val testList = List("a", "b", "c")

  "dummyKafkaMessage" should "build correct json" in {
    testList
      .map(x => (CanalEntryJsonHelper.dummyKafkaMessage(x)))
      .map { x =>
        println(x.getJsonValue); assert(TestContext.isJson(x.getJsonValue))
      }

  }
}
