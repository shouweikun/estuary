package com.neighborhood.aka.laplace.estuary.mongo.lifecycle

import akka.actor.{Actor, ActorLogging}
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle.SyncControllerMessage
import com.neighborhood.aka.laplace.estuary.mongo.task.Mongo2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/5/3.
  */
class OplogSinkerManager(
                          mongo2KafkaTaskInfoManager: Mongo2KafkaTaskInfoManager
                        ) extends Actor with ActorLogging {


  /**
    * 位置管理
    */
  lazy val positionHandler = mongo2KafkaTaskInfoManager.mongoOffsetHandler

  override def receive: Receive = {
    case SyncControllerMessage(msg) => msg match {
      case "start" => {
        context.become(online)
      }
    }
  }

  def online: Receive = {
    case message:KafkaMessage =>
  }
}
