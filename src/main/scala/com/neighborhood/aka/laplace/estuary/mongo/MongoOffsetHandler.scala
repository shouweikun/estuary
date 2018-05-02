package com.neighborhood.aka.laplace.estuary.mongo

import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import com.neighborhood.aka.laplace.estuary.core.task.PositionHandler
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2018/4/25.
  */
class MongoOffsetHandler(
                          val manager: ZooKeeperLogPositionManager,
                          val startMongoOffset: Option[MongoOffset] = None,
                          val destination: String = ""
                        ) extends PositionHandler[MongoOffset] {
  val logger = LoggerFactory.getLogger(classOf[MongoOffsetHandler])
  override def persistLogPosition(destination: String, logPosition: MongoOffset): Unit = ???

  override def findStartPosition(conn: DataSourceConnection): MongoOffset = ???
}
