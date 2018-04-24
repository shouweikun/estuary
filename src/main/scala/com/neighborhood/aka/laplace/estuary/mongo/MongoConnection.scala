package com.neighborhood.aka.laplace.estuary.mongo

import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection

/**
  * Created by john_liu on 2018/4/23.
  */
class MongoConnection(

                     )
  extends DataSourceConnection {
  override def connect(): Unit = {

  }

  override def reconnect(): Unit = {}

  override def disconnect(): Unit = {}

  override def isConnected: Boolean = ???

  override def fork: MongoConnection = ???
}
