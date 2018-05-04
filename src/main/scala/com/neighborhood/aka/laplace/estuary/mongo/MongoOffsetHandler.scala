package com.neighborhood.aka.laplace.estuary.mongo

import com.neighborhood.aka.laplace.estuary.core.util.ZooKeeperLogPositionManager
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import com.neighborhood.aka.laplace.estuary.core.task.PositionHandler
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2018/4/25.
  */
class MongoOffsetHandler(
                          val manager: ZooKeeperLogPositionManager[MongoOffset],
                          private val startMongoOffset: Option[MongoOffset] = None,
                          val destination: String = ""
                        ) extends PositionHandler[MongoOffset] {
  val logger = LoggerFactory.getLogger(classOf[MongoOffsetHandler])

  override def persistLogPosition(destination: String, logPosition: MongoOffset): Unit = {
    manager.persistLogPosition(destination, logPosition)
  }

  override def getlatestIndexBy(destination: String): MongoOffset = {
    manager.getLatestIndexBy(destination)
  }

  override def findStartPosition(conn: DataSourceConnection): MongoOffset
  = ???


}
