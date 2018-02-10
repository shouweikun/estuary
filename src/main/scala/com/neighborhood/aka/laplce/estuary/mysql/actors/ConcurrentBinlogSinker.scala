package com.neighborhood.aka.laplce.estuary.mysql.actors

import akka.actor.{Actor, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle.{SourceDataSinker, SyncControllerMessage}
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/2/9.
  */
class ConcurrentBinlogSinker(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) extends Actor with SourceDataSinker {

  //offline
  override def receive: Receive = {
    case SyncControllerMessage(msg) => {
      msg match {
        case "start" => {
          context.become(online)
        }
      }
    }
  }

  def online: Receive = {
    case list: List[CanalEntry.Entry] => {

    }
  }

  override var errorCountThreshold: Int = 3
  override var errorCount: Int = 0

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???
}

object ConcurrentBinlogSinker {
  def prop(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager):Props = {
    Props(new ConcurrentBinlogSinker(mysql2KafkaTaskInfoManager))
  }
}