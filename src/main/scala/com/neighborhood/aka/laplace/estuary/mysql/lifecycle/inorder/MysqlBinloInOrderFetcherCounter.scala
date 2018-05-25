package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.core.lifecycle.FetcherMessage
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager

import scala.util.Try

/**
  * Created by john_liu on 2018/5/9.
  */
class MysqlBinlogInOrderFetcherCounter(
                                        taskInfoManager: Mysql2KafkaTaskInfoManager
                                      ) extends Actor with ActorLogging {
  val syncTaskId: String = taskInfoManager.syncTaskId
  lazy val counter: Option[ActorRef] = taskInfoManager.processingCounter
  lazy val powerAdapter = taskInfoManager.powerAdapter
  override def receive: Receive = {

    case entry: CanalEntry.Entry => {
      def parseError = {
        log.error(s"parse row data error,id:${syncTaskId}");
        throw new Exception(s"parse row data error,id:${syncTaskId}")
      }

      val rowCount = Try(CanalEntry.RowChange.parseFrom(entry.getStoreValue)).toOption.getOrElse(parseError).getRowDatasCount
      val actRowCount = if (rowCount <= 0) 1 else rowCount
      counter.fold(log.error("processingManager cannot be null")) { ref => ref ! FetcherMessage(actRowCount) }
      powerAdapter.fold(log.warning(s"powerAdapter cannot be null ,id $syncTaskId"))(ref => ref ! FetcherMessage(s"$actRowCount"))
      }
  }

  override def preStart(): Unit = log.info(s"init fetcherSpecializedCounter,id:$syncTaskId ")
}

object MysqlBinlogInOrderFetcherCounter {
  def props(taskInfoManager: Mysql2KafkaTaskInfoManager): Props = Props(new MysqlBinlogInOrderFetcherCounter(taskInfoManager))

}
