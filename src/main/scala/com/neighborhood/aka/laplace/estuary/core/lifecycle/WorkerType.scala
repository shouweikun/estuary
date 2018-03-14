package com.neighborhood.aka.laplace.estuary.core.lifecycle

/**
  * Created by john_liu on 2018/3/14.
  */
object WorkerType extends Enumeration {
  type WorkerType = Value
  val SyncController = Value(0)
  val Fetcher = Value(1)
  val Batcher = Value(2)
  val Sinker = Value(3)
  val Listener = Value(4)
}