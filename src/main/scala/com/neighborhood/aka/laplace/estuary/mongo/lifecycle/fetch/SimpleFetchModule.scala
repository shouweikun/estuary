package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch

import com.mongodb.client.MongoCursor
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoOffset}
import org.bson.Document
import org.slf4j.LoggerFactory
import SimpleFetchModule._

import scala.util.Try

/**
  * Created by john_liu on 2019/2/28.
  *
  * 專門用於數據拉取的類
  *
  * 注意，在这个类的实现中 `mongoConnection` 的生命周期和`所有权`不归此类管理而是taskmanager统一管理
  *
  * @author neighborhood.aka.laplace
  * @note 线程不安全
  */
final class SimpleFetchModule(
                               mongoConnection: MongoConnection,
                               mongoOffset: Option[MongoOffset],
                               syncTaskId: String
                             ) {

  private var iterator: Option[MongoCursor[Document]] = None
  private var isStart_ = false

  def start(): Unit = {
    logger.info(s"simple fetch module start,id:$syncTaskId")
    assert(mongoConnection.isConnected)
    iterator = Option(mongoOffset.fold(mongoConnection.getOplogIterator())(mongoConnection.getOplogIterator(_)))
    isStart_ = true
  }

  def fetch: Option[Document] = iterator.fold(throw new RuntimeException(s"iter is null when fetch oplog doc,id:$syncTaskId")) { iter => if (iter.hasNext) Option(iter.next()) else None }

  /**
    * 关闭，不要扔出异常
    */
  def close(): Unit = Try {
    logger.info(s"simple fetch module try to close,id:$syncTaskId")
    iterator.map(_.close())
    iterator = None
    isStart_ = false
  }


  def reconnect: Unit = {
    logger.info(s"simple fetch module try to restart,id:$syncTaskId")
    close()
    start()
  }

  def isStart = isStart_

}

object SimpleFetchModule {
  private[SimpleFetchModule] val logger = LoggerFactory.getLogger(classOf[SimpleFetchModule])
}
