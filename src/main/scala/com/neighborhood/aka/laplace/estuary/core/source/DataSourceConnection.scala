package com.neighborhood.aka.laplace.estuary.core.source

import java.io.IOException

/**
  * Created by john_liu on 2018/3/21.
  */
trait DataSourceConnection {
  @throws[IOException]
  def connect(): Unit

  @throws[IOException]
  def reconnect(): Unit

  @throws[IOException]
  def disconnect(): Unit

  def isConnected: Boolean
}
