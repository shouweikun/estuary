package com.neighborhood.aka.laplace.estuary.mongo.source

import com.neighborhood.aka.laplace.estuary.bean.resource.{DataSourceBase, MongoSourceBean}
import com.neighborhood.aka.laplace.estuary.core.task.SourceManager
import com.neighborhood.aka.laplace.estuary.mongo.util.OplogOffsetHandler

/**
  * Created by john_liu on 2019/2/28.
  */
trait MongoSourceManagerImp extends SourceManager[MongoConnection] {
  override def sourceBean: MongoSourceBean

  override def buildSource: MongoConnection = {
    new MongoConnection(sourceBean)
  }

  def positionHandler: OplogOffsetHandler = positionHandler_

  private lazy val positionHandler_ = buildMongoOffsetPositionHandler

  def buildMongoOffsetPositionHandler: OplogOffsetHandler = {
    ???
  }
}
