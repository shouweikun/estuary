package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.count

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.core.lifecycle._
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.ProcessingCountPrototype
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.count.MysqlBinlogInOrderProcessingCounterCommand._

/**
  * Created by john_liu on 2018/5/8.
  * 计数器，用于计算数量
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlInOrderProcessingCounter(
                                           override val taskManager: TaskManager
                                         ) extends ProcessingCountPrototype {
  override val syncTaskId = taskManager.syncTaskId

  override def receive: Receive = {
    case FetcherMessage(MysqlBinlogInOrderProcessingCounterUpdateCount(x: Long)) => addFetchCount(x)
    case BatcherMessage(MysqlBinlogInOrderProcessingCounterUpdateCount(x: Long)) => addBatchCount(x)
    case SinkerMessage(MysqlBinlogInOrderProcessingCounterUpdateCount(x: Long)) => {
      addSinkCount(x)
    }
    case SyncControllerMessage(MysqlBinlogInOrderProcessingCounterComputeCount) => {
      updateRecord(SettingConstant.COMPUTE_COUNT_CONSTANT)
      log.debug(s"set fetch count $fetchCount,batch count $batchCount,sink count $sinkCount,id:$syncTaskId")
    }
  }


}

object MysqlInOrderProcessingCounter {
  def props(taskManager: TaskManager): Props = Props(new MysqlInOrderProcessingCounter(taskManager))
}
