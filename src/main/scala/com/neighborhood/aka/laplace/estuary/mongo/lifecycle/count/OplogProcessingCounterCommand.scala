package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.count

/**
  * Created by john_liu on 2019/1/11.
  */
sealed trait OplogProcessingCounterCommand

object OplogProcessingCounterCommand {

  case class OplogProcessingCounterUpdateCount(count: Long) extends OplogProcessingCounterCommand

  case object OplogProcessingCounterComputeCount extends OplogProcessingCounterCommand

}