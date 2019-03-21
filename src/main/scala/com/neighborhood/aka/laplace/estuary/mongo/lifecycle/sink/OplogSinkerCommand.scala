package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink

import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait OplogSinkerCommand

object OplogSinkerCommand {

  case object OplogSinkerStart extends OplogSinkerCommand

  case class OplogSinkerGetAbnormal(e: Throwable, offset: Option[MongoOffset]) extends OplogSinkerCommand

  case object OplogSinkerCheckBatch extends OplogSinkerCommand
  case object OplogSinkerCheckFlush extends OplogSinkerCommand

  case object OplogSinkerCollectOffset extends OplogSinkerCommand

  case object OplogSinkerSendOffset extends OplogSinkerCommand

}


