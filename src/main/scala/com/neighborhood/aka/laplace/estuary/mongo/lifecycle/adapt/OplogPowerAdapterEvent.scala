package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt

import com.neighborhood.aka.laplace.estuary.core.eventsource.EstuaryEvent

/**
  * Created by john_liu on 2019/1/19.
  */
sealed trait OplogPowerAdapterEvent extends EstuaryEvent

object OplogPowerAdapterEvent {
  case class OplogPowerAdapterCostUpdated(cost: Long) extends OplogPowerAdapterEvent

  case class OplogPowerAdapterFetcherCountUpdated(count: Long) extends OplogPowerAdapterEvent //special case 因为fetcher的计数特性

  case object OplogPowerAdapterControlled$ extends OplogPowerAdapterEvent

  case object OplogPowerAdapterCostComputed$ extends OplogPowerAdapterEvent

  case class OplogPowerAdapterFetchDelayed(delay:Long) extends OplogPowerAdapterEvent

}
