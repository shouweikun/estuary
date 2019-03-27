package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoSourceManagerImp

/**
  * Created by john_liu on 2019/3/27.
  */
object OplogFetcher {

  def buildOplogFetcher(name: String, downStream: ActorRef, taskManager: TaskManager): Props = name match {
    case SimpleOplogFetcher4sda.name => SimpleOplogFetcher4sda.props(taskManager.asInstanceOf[MongoSourceManagerImp with TaskManager], downStream)
    case SimpleOplogFetcher.name => SimpleOplogFetcher.props(taskManager.asInstanceOf[MongoSourceManagerImp with TaskManager], downStream)
    case _ => throw new RuntimeException(s"$name is not supported currently")
  }
}
