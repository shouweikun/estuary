package com.neighborhood.aka.laplce.estuary.mysql

import com.neighborhood.aka.laplce.estuary.core.lifecycle.Status.Status

/**
  * Created by john_liu on 2018/2/3.
  */
package object lifecycle {

  case class BinlogPositionInfo(journalName: String, offest: Long)

  def changeFunc(status: Status)(workerType: String, mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager): Unit = {
    workerType match {
      case "listener" => mysql2KafkaTaskInfoManager.heartBeatListenerStatus = status
      case "batcher" => mysql2KafkaTaskInfoManager.batcherStatus = status
      case "sinker" => mysql2KafkaTaskInfoManager.sinkerStatus = status
      case "fetcher" => mysql2KafkaTaskInfoManager.fetcherStatus =status
      case "controller" => mysql2KafkaTaskInfoManager.syncControllerStatus = status
    }
  }
}
