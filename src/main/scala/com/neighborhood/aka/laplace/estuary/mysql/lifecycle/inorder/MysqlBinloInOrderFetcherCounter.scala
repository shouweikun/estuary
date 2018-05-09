package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.core.lifecycle.SyncControllerMessage

import scala.util.Try

/**
  * Created by john_liu on 2018/5/9.
  */
class MysqlBinlogInOrderFetcherCounter(
                                     syncTaskId:String,
                                       counter:ActorRef
                                     ) extends Actor with ActorLogging{
  override def receive: Receive = {
    case entry:CanalEntry.Entry => {
      def parseError = {
        log.error(s"parse row data error,id:${syncTaskId}");
        throw new Exception(s"parse row data error,id:${syncTaskId}")
      }

      val rowCount = Try(CanalEntry.RowChange.parseFrom(entry.getStoreValue)).toOption.getOrElse(parseError).getRowDatasCount
      counter ! SyncControllerMessage(rowCount)
    }
  }

  override def preStart(): Unit = log.info(s"init fetcherSpecializedCounter,id:$syncTaskId ")
}
object MysqlBinlogInOrderFetcherCounter {
  def props( syncTaskId:String, counter:ActorRef):Props = Props(new MysqlBinlogInOrderFetcherCounter(syncTaskId,counter))

}
