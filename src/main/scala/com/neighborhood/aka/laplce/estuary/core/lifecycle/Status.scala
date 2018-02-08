package com.neighborhood.aka.laplce.estuary.core.lifecycle

/**
  * Created by john_liu on 2018/2/6.
  */
case object Status extends Enumeration {
  type Status = Value
  val OFFLINE = Value(0)
  val BUSY = Value(1)
  val SUSPEND = Value(2)
  val FREE    = Value(3)
  val ERROR   = Value(4)

}

