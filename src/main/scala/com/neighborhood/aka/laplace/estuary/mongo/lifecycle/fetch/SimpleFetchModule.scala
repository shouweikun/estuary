package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch

import com.mongodb.client.MongoCursor
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoOffset}
import org.bson.Document
import org.slf4j.LoggerFactory
import SimpleFetchModule._

/**
  * Created by john_liu on 2019/2/28.
  *
  * 專門用於數據拉取的類
  *
  * @author neighborhood.aka.laplace
  * @note 线程不安全
  */
final class SimpleFetchModule(
                               mongoConnection: MongoConnection,
                               mongoOffset: Option[MongoOffset]
                             ) {

  private var iterator: Option[MongoCursor[Document]] = None

  def start(): Unit = {
    logger.info("simple fetch module start")
    assert(mongoConnection.isConnected)
    iterator = Option(mongoOffset.fold(mongoConnection.getOplogIterator())(mongoConnection.getOplogIterator(_)))
  }

  def fetch: Option[Document] = iterator.fold(throw new RuntimeException("iter is null when fetch oplog doc")) { iter => if (iter.hasNext) Option(iter.next()) else None }

}

object SimpleFetchModule {
  private[SimpleFetchModule] val logger = LoggerFactory.getLogger(classOf[SimpleFetchModule])
}
