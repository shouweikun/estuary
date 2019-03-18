package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.core.sink.hbase.HBaseSinkManager
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.hbase.{DefaultOplogKeyHBaseSinkerManager, OplogKeyHBaseByNameSinkerManager}
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2019/3/18.
  */
class OplogSinkerManager {

}

object OplogSinkerManager {
  private[OplogSinkerManager] lazy val logger = LoggerFactory.getLogger(classOf[OplogSinkerManager])

  def builgOplogSinkerManager(nameToLoad: String, taskManager: TaskManager): Props = {
    case DefaultOplogKeyHBaseSinkerManager.name => DefaultOplogKeyHBaseSinkerManager.props(taskManager.asInstanceOf[HBaseSinkManager with TaskManager])
    case OplogKeyHBaseByNameSinkerManager.name => OplogKeyHBaseByNameSinkerManager.props(taskManager.asInstanceOf[HBaseSinkManager with TaskManager])
    case _ => throw new IllegalArgumentException(s"$nameToLoad is not supported currently when builgOplogSinkerManager")
  }
}
