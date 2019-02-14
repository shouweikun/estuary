package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.bean.exception.fetch.{EmptyEntryException, NullOfDataSourceConnectionException}
import com.neighborhood.aka.laplace.estuary.mysql.source.{MysqlConnection, MysqlSourceManagerImp}
import com.neighborhood.aka.laplace.estuary.mysql.utils.{CanalEntryTransUtil, MysqlBinlogParser}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by john_liu on 2018/7/3.
  */
final class FetchEntryHandler(
                               taskManager: MysqlSourceManagerImp
                             ) {

  /**
    * 任务id
    */
  val syncTaskId = taskManager.syncTaskId
  /**
    * 库同步开始时间
    */
  val taskStartTime = taskManager.syncStartTime
  /**
    * batchCount
    */
  val batchCount = taskManager.batchThreshold
  /**
    * 是否开启schema模块
    */
  val schemaComponentIsOn = taskManager.schemaComponentIsOn
  /**
    * binlogParser 解析binlog
    */
  lazy val binlogParser: MysqlBinlogParser = taskManager.binlogParser
  /**
    * 日志
    */
  val log = LoggerFactory.getLogger(s"FetchEntryHandler-$syncTaskId")
  /**
    * 是否在拉取數據
    */
  private var isFetching_ = false

  def isFetching = isFetching_

  def switchToFetching = isFetching_ = true

  def switchToFree = isFetching_ = false

  /**
    * 阻塞式數據拉取
    *
    * @param batchCount
    * @param mysqlConnection
    * @param binlogParser
    * @return
    */
  def blockingHandleFetchTask(batchCount: Long = this.batchCount)
                             (
                               mysqlConnection: Option[MysqlConnection],
                               binlogParser: MysqlBinlogParser = this.binlogParser
                             ): List[(CanalEntry.Entry, Long)] = {
    lazy val conn = mysqlConnection.getOrElse(throw new NullOfDataSourceConnectionException(s"mysqlConnection is null when fetch data,id:$syncTaskId"))

    @tailrec
    def loopFetch(acc: => List[(CanalEntry.Entry, Long)] = List.empty): List[(CanalEntry.Entry, Long)] = {
      if (acc.size >= batchCount)
        acc
      else {
        lazy val entryAndCost = blockingFetchOne(conn, binlogParser)
        lazy val entry = entryAndCost._1
        lazy val nextAcc = entryAndCost :: acc
        if (CanalEntryTransUtil.isDdl(entry)) {
          nextAcc
        } else loopFetch(nextAcc)

      }
    }

    loopFetch()
  }


  /**
    * 拉取一条符合要求的entry
    * 必定為Some(entry)
    *
    * @param mysqlConnection
    * @param binlogParser
    * @param startTimestamp
    * @return
    */
  private def blockingFetchOne(
                                mysqlConnection: MysqlConnection,
                                binlogParser: MysqlBinlogParser,
                                startTimestamp: Long = System.nanoTime() / 1000000
                              ): (CanalEntry.Entry, Long) = {

    lazy val endTimestamp = System.nanoTime() / 1000000
    mysqlConnection
      .blockingFetchUntilDefined(filterEntry(_))(binlogParser)
      .fold(throw new EmptyEntryException(s"unexcepted empty entry when fetchOne,id:$syncTaskId"))((_, endTimestamp - startTimestamp))
  }

  /**
    * 非阻塞方式拉取数据
    * 拉取不到数据时返回None
    *
    * @param mysqlConnection
    * @param binlogParser
    * @param startTimestamp
    * @return
    */

  private def nonBlockingFetchOne(
                                   mysqlConnection: MysqlConnection,
                                   binlogParser: MysqlBinlogParser,
                                   startTimestamp: Long = System.currentTimeMillis()
                                 )(implicit ec: ExecutionContext = null): Future[CanalEntry.Entry] = {

    mysqlConnection.nonBlockingFetch(filterEntry)(binlogParser)(ec)
  }

  /**
    * @param entryOption 得到的entry
    *                    对entry在类型级别上进行过滤
    */
  private def filterEntry(entryOption: Option[CanalEntry.Entry]): Boolean = {

    entryOption.fold(false) {
      entry =>
        //我们只要rowdata和Transactionend
        if (
          CanalEntryTransUtil.isDdl(entry) || CanalEntryTransUtil.isDml(entry.getHeader.getEventType) || CanalEntryTransUtil.isTransactionEnd(entry)
        ) true else false
    }
  }
}

object FetchEntryHandler {
  def apply(
             taskManager: MysqlSourceManagerImp
           ): FetchEntryHandler = new FetchEntryHandler(taskManager)
}