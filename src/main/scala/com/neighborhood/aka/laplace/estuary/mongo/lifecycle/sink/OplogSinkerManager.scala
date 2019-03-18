package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2019/3/18.
  */
class OplogSinkerManager {

}

object OplogSinkerManager {
  private[OplogSinkerManager] lazy val logger = LoggerFactory.getLogger(classOf[OplogSinkerManager])

  def builgOplogSinkerManager(nameToLoad: String, taskManager: TaskManager, sinker: ActorRef): Props = {
   ???
  }
}
