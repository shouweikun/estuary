package com.neighborhood.aka.laplace.estuary.mongo.source

import com.neighborhood.aka.laplace.estuary.bean.resource.MongoSourceBean
import com.neighborhood.aka.laplace.estuary.core.task.SourceManager
import com.neighborhood.aka.laplace.estuary.core.util.zookeeper.{EstuaryStringZookeeperManager, EstuaryZkClient}
import com.neighborhood.aka.laplace.estuary.mongo.util.OplogOffsetHandler

/**
  * Created by john_liu on 2019/2/28.
  */
trait MongoSourceManagerImp extends SourceManager[MongoConnection] {

  def syncTaskId: String

  def offsetZookeeperServer: String

  def startMongoOffset: Option[MongoOffset]

  override def sourceBean: MongoSourceBean

  override def buildSource: MongoConnection = {
    new MongoConnection(sourceBean)
  }

  def positionHandler: OplogOffsetHandler = positionHandler_

  private lazy val positionHandler_ = buildMongoOffsetPositionHandler

  def buildMongoOffsetPositionHandler: OplogOffsetHandler = {
    val zkClient = new EstuaryZkClient(offsetZookeeperServer)
    val zkManager = new EstuaryStringZookeeperManager(zkClient)
    new OplogOffsetHandler(zkManager, syncTaskId, startMongoOffset)
  }
}
