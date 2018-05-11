package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.{Actor, ActorLogging, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SyncControllerMessage, _}
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/5/8.
  */
class MysqlInOrderProcessingCounter(
                                     override val taskManager: Mysql2KafkaTaskInfoManager
                                   ) extends ProcessingCounter with Actor with ActorLogging {
  val syncTaskId = taskManager.syncTaskId

  override def receive: Receive = {
    case FetcherMessage(x: Long) => addFetchCount(x)
    case FetcherMessage(x: Int) => addFetchCount(x)
    case BatcherMessage(x: Long) => addBatchCount(x)
    case BatcherMessage(x: Int) => addBatchCount(x)
    case SinkerMessage(x: Long) => addSinkCount(x)
    case SinkerMessage(x: Int) => addSinkCount(x)
    case SyncControllerMessage("count") => {
      updateRecord
      log.debug(s"set fetch count $fetchCount,batch count $batchCount,sink count $sinkCount,id:$syncTaskId")
    }
  }


}

object MysqlInOrderProcessingCounter {
  def props(taskManager: Mysql2KafkaTaskInfoManager): Props = Props(new MysqlInOrderProcessingCounter(taskManager))
}
