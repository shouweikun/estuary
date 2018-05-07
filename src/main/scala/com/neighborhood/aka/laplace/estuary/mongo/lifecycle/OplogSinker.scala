package com.neighborhood.aka.laplace.estuary.mongo.lifecycle

import akka.actor.{Actor, ActorLogging}
import com.neighborhood.aka.laplace.estuary.mongo.task.Mongo2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/5/2.
  */
class OplogSinker(
                   mongo2KafkaTaskInfoManager: Mongo2KafkaTaskInfoManager
                 ) extends Actor with ActorLogging {
  override def receive: Receive = ???
}
