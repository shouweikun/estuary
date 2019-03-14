package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch

/**
  * Created by john_liu on 2019/3/13.
  */
sealed trait OplogFetcherEvent
final object OplogFetcherEvent {
case class OplogFetcherActiveChecked(ts:Long = System.currentTimeMillis()) extends OplogFetcherEvent
}
