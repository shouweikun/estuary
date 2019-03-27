package com.neighborhood.aka.laplace.estuary.mongo.source

import com.neighborhood.aka.laplace.estuary.bean.resource.MongoSourceBean
import com.neighborhood.aka.laplace.estuary.core.task.SourceManager
import com.neighborhood.aka.laplace.estuary.core.util.zookeeper.{EstuaryStringZookeeperManager, EstuaryZkClient}
import com.neighborhood.aka.laplace.estuary.mongo.util.OplogOffsetHandler
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by john_liu on 2019/2/28.
  */
trait MongoSourceManagerImp extends SourceManager[MongoConnection] {

  protected lazy val logger = LoggerFactory.getLogger(classOf[MongoSourceManagerImp])

  def syncTaskId: String

  def offsetZookeeperServer: String

  def startMongoOffset: Option[MongoOffset]

  override def sourceBean: MongoSourceBean

  override def buildSource: MongoConnection = {
    new MongoConnection(sourceBean)
  }

  override def startSource: Unit = {
    logger.info(s"start source,id:$syncTaskId")
    super.startSource
    positionHandler.start()
  }


  /**
    * 停止所有资源
    */
  override def closeSource: Unit = {
    logger.info(s"close source,id:$syncTaskId")
    super.closeSource
    positionHandler.close()
  }

  def positionHandler: OplogOffsetHandler = positionHandler_

  private lazy val positionHandler_ = buildMongoOffsetPositionHandler

  def buildMongoOffsetPositionHandler: OplogOffsetHandler = {
    val zkClient = EstuaryZkClient.getZkClient(offsetZookeeperServer)
    val zkManager = new EstuaryStringZookeeperManager(zkClient)
    new OplogOffsetHandler(zkManager, syncTaskId, startMongoOffset)
  }
}
