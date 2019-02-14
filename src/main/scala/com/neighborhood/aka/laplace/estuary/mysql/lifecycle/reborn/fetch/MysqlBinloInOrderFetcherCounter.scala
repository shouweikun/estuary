package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.core.lifecycle.FetcherMessage
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.count.MysqlBinlogInOrderProcessingCounterCommand.MysqlBinlogInOrderProcessingCounterUpdateCount
import com.neighborhood.aka.laplace.estuary.mysql.utils.{CanalEntryTransHelper, CanalEntryTransUtil}

import scala.util.Try

/**
  * Created by john_liu on 2018/5/9.
  * 专门用于处理Fetcher部分的计数
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInOrderFetcherCounter(
                                        taskManager: TaskManager
                                      ) extends Actor with ActorLogging {
  /**
    * 同步任务Id
    */
  val syncTaskId: String = taskManager.syncTaskId
  /**
    * 计数器
    */
  lazy val counter: Option[ActorRef] = taskManager.processingCounter
  /**
    * 功率控制器
    */
  lazy val powerAdapter: Option[ActorRef] = taskManager.powerAdapter


  override def receive: Receive = {

    case entry: CanalEntry.Entry =>countEntry(entry)
  }

  /**
    * 計算entry中row的數量
    * @param entry entry
    */
  private def countEntry(entry: CanalEntry.Entry):Unit =  {
    def parseError = {
      log.error(s"parse row data error,id:${syncTaskId}");
      throw new Exception(s"parse row data error,id:${syncTaskId}")
    }

    lazy val rowCount = Try(CanalEntry.RowChange.parseFrom(entry.getStoreValue)).toOption.getOrElse(parseError).getRowDatasCount
    //如果是ddl语句或者是事务结束位的话，不论rowCount为多少计数都为1
    val actRowCount = if (
      entry.getEntryType == CanalEntry.EntryType.TRANSACTIONEND || CanalEntryTransUtil.isDdl(entry.getHeader.getEventType)
    ) 1 else rowCount
    //如果还是出现了rowCount为零的情况，输出错误日志以待分析原因
    if (actRowCount <= 0) log.error(s"Oops,Actual Row Count:$actRowCount,eventType:${CanalEntryTransHelper.headerToJson(entry.getHeader)},$syncTaskId")
    //向计数器发送计数
    counter.fold(log.error("processingManager cannot be null")) { ref => ref ! FetcherMessage(MysqlBinlogInOrderProcessingCounterUpdateCount(actRowCount)) }
    //向功率调节器发送计数
    powerAdapter.fold(log.warning(s"powerAdapter cannot be null ,id $syncTaskId"))(ref => ref ! FetcherMessage(s"$actRowCount"))

  }
  override def preStart(): Unit = log.info(s"init fetcherSpecializedCounter,id:$syncTaskId ")
}

object MysqlBinlogInOrderFetcherCounter {
  def props(taskInfoManager: TaskManager): Props = Props(new MysqlBinlogInOrderFetcherCounter(taskInfoManager))

}
