package com.neighborhood.aka.laplace.estuary.core.snapshot

/**
  * Created by john_liu on 2018/7/9.
  */
object SnapshotStatus  extends Enumeration {
  type SnapshotStatus = Value
  val NO_SNAPSHOT = Value(0)
  val ACCUMULATING_DATA = Value(1)
  val SUSPEND_4_WAKEUP = Value(2)
  val UNKNOWN  = Value(3)


}
