package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplace.estuary.bean.exception.fetch.{CannotFindOffsetException, NullOfDataSourceConnectionException}
import com.neighborhood.aka.laplace.estuary.core.util.JavaCommonUtil
import com.neighborhood.aka.laplace.estuary.mysql.source.{MysqlConnection, MysqlSourceManagerImp}
import com.neighborhood.aka.laplace.estuary.mysql.utils.MysqlBinlogParser
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2018/7/3.
  * 该类负责Fetcher从offline切换状态时所需要执行的方法
  * 寻找对应的offset
  * 准备链接
  * 初始化schema
  *
  * @author neighborhood.aka.laplace
  */
final class FetchContextInitializer(
                                     taskManager: MysqlSourceManagerImp
                                   ) {
  /**
    * 任务id
    */
  lazy val syncTaskId = taskManager.syncTaskId
  /**
    * 寻址处理器
    */
  lazy val logPositionHandler = taskManager.logPositionHandler
  /**
    * binlogParser 解析binlog
    */
  lazy val binlogParser: MysqlBinlogParser = taskManager.binlogParser
  /**
    * 日志
    */
  lazy val log = LoggerFactory.getLogger(s"FetchContextInitializer-$syncTaskId")
  /**
    * 元数据管理是否开启
    */
  lazy val schemaComponentIsOn = taskManager.schemaComponentIsOn

  /**
    * 寻址开始的binlog offset
    *
    * @param mysqlConnection
    * @return 寻找开始binlog Offset
    */
  def findStartPosition(mysqlConnection: => MysqlConnection): EntryPosition = {
    lazy val conn = mysqlConnection.fork
    log.info(s"start find start Position,id:$syncTaskId")
    lazy val re = logPositionHandler.findStartPosition(conn)
    lazy val entryPositionIsInvalid = Option(re).fold(true)(pos => pos.getJournalName.trim.equals(""))

    //如果开始位置是空，抛错
    if (entryPositionIsInvalid) throw new CannotFindOffsetException(
      {
        log.error(s"fetcher find entryPosition is null,id:$syncTaskId");
        s"entryPosition is null when find position,id:$syncTaskId"
      }
    ) else re //否则返回寻找到的值

  }

  /**
    * 主要是应对tableIdNotFound的情况
    * 进行transcational寻址
    *
    * @param mysqlConnection
    * @return
    */
  def findStartPositionWithTransaction(mysqlConnection: Option[MysqlConnection]) = Option(mysqlConnection.fold(throw new NullOfDataSourceConnectionException(s"mysqlConnection is null when find Start PositionWithinTransaction,id:$syncTaskId"))(conn => logPositionHandler.findStartPositionWithinTransaction(conn)))

  /**
    * 在Start时调用的方法
    * 1.初始化Schema元数据
    * 2.存下logPosition
    *
    * @param mysqlConnection
    * @param thePosition
    */
  def onPrepareStart(mysqlConnection: Option[MysqlConnection], thePosition: EntryPosition) = {

    log.info(s"MysqlBinlogInOrderDirectFetcher process onPrepareStart,to load schema info and save logPosition,id:$syncTaskId")
    log.info(s"fetcher find start position,binlogFileName:${thePosition.getJournalName},${thePosition.getPosition},id:$syncTaskId")
    mysqlConnection.fold(throw new NullOfDataSourceConnectionException(s"cannot find mysqlConnection when switch2Start,id:$syncTaskId")) { conn => if (!conn.isConnected) conn.connect() }
    logPositionHandler.persistLogPosition(syncTaskId, thePosition)
  }

  /**
    * 准备拉取数据
    * 1.preDump
    * 2.dump
    *
    * @param mysqlConnection
    * @param entryPosition
    */
  def preFetch(mysqlConnection: => Option[MysqlConnection])(entryPosition: Option[EntryPosition]): Unit = {
    log.info(s"start preFetch,id:$syncTaskId")
    preDump(mysqlConnection)
    dump(mysqlConnection)(entryPosition)
  }

  /**
    * 执行mysqlConnection的predump
    *
    * @param mysqlConnection
    */
  private def preDump(mysqlConnection: => Option[MysqlConnection]): Unit = mysqlConnection.fold {
    throw new NullOfDataSourceConnectionException({
      log.error(s"fetcher mysqlConnection cannot be null when predump,id:$syncTaskId");
      s"fetcher mysqlConnection cannot be null,id:$syncTaskId"
    })
  } {
    conn =>
      log.debug(s"fetcher predump,id:$syncTaskId")
      MysqlConnection.preDump(conn)(binlogParser);
      conn.reconnect()
  }

  /**
    * 执行mysqlConnection的Dump命令
    *
    * @param mysqlConnection
    * @param entryPosition
    */
  private def dump(mysqlConnection: => Option[MysqlConnection])(entryPosition: Option[EntryPosition]) = {
    val startPosition = entryPosition.fold(throw new CannotFindOffsetException(s"fetcher find entryPosition is null,id:$syncTaskId"))(x => x)

    if (JavaCommonUtil.isEmpty(startPosition.getJournalName) && Option(startPosition.getTimestamp).isEmpty) {
      log.error(s"unsupported operation: dump by timestamp is not supported yet,id:$syncTaskId")
      throw new UnsupportedOperationException(s"unsupported operation: dump by timestamp is not supported yet,id:$syncTaskId")
    } else {
      log.info(s"start dump binlog,id:$syncTaskId")

      mysqlConnection.fold(throw new NullOfDataSourceConnectionException(s"mysqlConnection is null when start to dump,id:$syncTaskId"))(conn => MysqlConnection.dump(startPosition.getJournalName, startPosition.getPosition)(conn))
    }
  }

}

object FetchContextInitializer {
  def apply(
             taskManager: MysqlSourceManagerImp
           ): FetchContextInitializer = new FetchContextInitializer(taskManager)
}