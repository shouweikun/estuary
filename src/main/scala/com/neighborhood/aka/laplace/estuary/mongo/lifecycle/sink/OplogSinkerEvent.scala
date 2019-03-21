package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink

import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset

/**
  * Created by john_liu on 2019/3/21.
  */
sealed trait OplogSinkerEvent

object OplogSinkerEvent {

  case class OplogSinkerOffsetCollected(offset: MongoOffset) extends OplogSinkerEvent

}
