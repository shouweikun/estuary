package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt

import com.neighborhood.aka.laplace.estuary.core.eventsource.EstuaryCommand

/**
  * Created by john_liu on 2018/10/10.
  */
sealed trait OplogPowerAdapterCommand extends EstuaryCommand

object OplogPowerAdapterCommand {

  case class OplogPowerAdapterUpdateCost(cost: Long) extends OplogPowerAdapterCommand

  case class OplogPowerAdapterUpdateFetcherCount(count: Long) extends OplogPowerAdapterCommand //special case 因为fetcher的计数特性

  case object OplogPowerAdapterControl extends OplogPowerAdapterCommand

  case object OplogPowerAdapterComputeCost extends OplogPowerAdapterCommand

  case class OplogPowerAdapterDelayFetch(delay:Long) extends OplogPowerAdapterCommand

}

