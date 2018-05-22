package com.neighborhood.aka.laplace.estuary.core.lifecycle.worker

/**
  * Created by john_liu on 2018/2/6.
  */
case object Status extends Enumeration {
  type Status = Value
  val OFFLINE = Value(0)
  val ONLINE = Value(1)
  val SUSPEND = Value(2)
  val ERROR   = Value(4)
  val RESTARTING = Value(5)

}

