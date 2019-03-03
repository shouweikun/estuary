package com.neighborhood.aka.laplace.estuary.mongo.util

import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import com.neighborhood.aka.laplace.estuary.core.task.PositionHandler
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset

/**
  * Created by john_liu on 2019/3/1.
  *
  * mongo的offset记录器
  * @author neighborhood.aka.laplace
  */
final class OplogOffsetHandler extends PositionHandler[MongoOffset]{
  override def persistLogPosition(destination: String, logPosition: MongoOffset): Unit = ???

  override def getlatestIndexBy(destination: String): MongoOffset = ???

  override def findStartPosition(conn: DataSourceConnection): MongoOffset = ???
}
