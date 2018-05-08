package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.{Actor, ActorLogging}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, FetcherMessage, ProcessingCounter, SinkerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2018/5/8.
  */
class MysqlInOrderProcessingCounter(
                                     override val taskManager: TaskManager
                                   ) extends ProcessingCounter with Actor with ActorLogging {
  val syncTaskId = taskManager.syncTaskId

  override def receive: Receive = {
    case FetcherMessage(x: Long) => addFetchCount(x)
    case FetcherMessage(x: Int) => addFetchCount(x)
    case BatcherMessage(x: Long) => addBatchCount(x)
    case BatcherMessage(x: Int) => addBatchCount(x)
    case SinkerMessage(x: Long) => addSinkCount(x)
    case SinkerMessage(x: Int) => addSinkCount(x)
    case "count" => {
      updateRecord
      log.debug(s"set fetch count $fetchCount,batch count $batchCount,sink count $sinkCount,id:$syncTaskId")
    }
  }


}
