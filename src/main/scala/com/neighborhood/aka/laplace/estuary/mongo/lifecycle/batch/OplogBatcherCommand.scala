package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait OplogBatcherCommand

object OplogBatcherCommand{
  case object OplogBatcherStart extends OplogBatcherCommand

  case object OplogBatcherCheckHeartbeats extends OplogBatcherCommand
}

