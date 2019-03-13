package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait OplogFetcherCommand

object OplogFetcherCommand {
  case object OplogFetcherStart extends OplogFetcherCommand

  case object OplogFetcherRestart extends OplogFetcherCommand

  case object OplogFetcherSuspend extends OplogFetcherCommand

  case object OplogFetcherBusy extends OplogFetcherCommand

  case object OplogFetcherFree extends OplogFetcherCommand

  case object OplogResume extends OplogFetcherCommand

  case object OplogFetcherPrefetch extends OplogFetcherCommand

  case object OplogFetcherFetch extends OplogFetcherCommand

  case object OplogFetcherNonBlockingFetch extends OplogFetcherCommand

  case class OplogFetcherUpdateDelay(delay: Long) extends OplogFetcherCommand

  case object OplogFetcherCheckActive extends OplogFetcherCommand

}