package com.neighborhood.aka.laplace.estuary.mongo.util

import java.util.concurrent.atomic.AtomicBoolean

import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import com.neighborhood.aka.laplace.estuary.core.task.PositionHandler
import com.neighborhood.aka.laplace.estuary.core.util.zookeeper.EstuaryStringZookeeperManager
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset

/**
  * Created by john_liu on 2019/3/1.
  *
  * mongo的offset记录器
  *
  * @note 非线程安全
  * @author neighborhood.aka.laplace
  */
final class OplogOffsetHandler(
                                private val zkManager: EstuaryStringZookeeperManager
                              ) extends PositionHandler[MongoOffset] {

  private val connectionStatus = new AtomicBoolean()

  override def start(): Unit = {
    zkManager.start()
    connectionStatus.set(true)
  }

  override def isStart: Boolean = connectionStatus.get()

  override def close(): Unit = {
    zkManager.stop()
    connectionStatus.set(false)
  }

  override def persistLogPosition(destination: String, logPosition: MongoOffset): Unit = {

  }

  override def getlatestIndexBy(destination: String): MongoOffset = ???

  override def findStartPosition(conn: DataSourceConnection): MongoOffset = ???
}
