package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

import akka.actor.{ActorRef, Props}
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.SimpleDdlParser
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.EventType
import com.neighborhood.aka.laplace.estuary.core.lifecycle.FetcherMessage
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.record.MysqlBinlogInOrderRecorderCommand.MysqlBinlogInOrderRecorderSaveLatestPosition
import com.neighborhood.aka.laplace.estuary.mysql.schema.SdaSchemaMappingRule
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2MysqlTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.utils.{CanalEntryTransHelper, CanalEntryTransUtil}

/**
  * Created by john_liu on 2019/1/16.
  *
  * sda专用的SimpleFetcher
  *
  * @author neighborhood.aka.laplace
  */
final class SdaMysqlBinlogInOrderDirectFetcher(
                                                override val taskManager: Mysql2MysqlTaskInfoManager,
                                                override val downStream: ActorRef) extends MysqlBinlogInOrderDirectFetcher(taskManager, downStream) {

  private lazy val sink: MysqlSinkFunc = taskManager.sink

  private lazy val rule: SdaSchemaMappingRule = taskManager.tableMappingRule

  private lazy val positionRecorder: Option[ActorRef] = taskManager.positionRecorder
  //  private lazy val ALTER_PATTERN = """^\s*[a/A][l/L][t/T][e/E][r/R]\s+[T/t][a/A][b/B][l/L][e/E]\s*(\w+).*""".r

  /**
    * SDA专用DDL处理
    *
    * 不论对于何种ddl,都会尝试发起SDA改名，如果未能找到,警告，并使用原始名称进行执行
    *
    * @param entry
    * @author neighborhood.aka.laplace
    */
  override protected def executeDdl(entry: CanalEntry.Entry): Unit = {
    log.info(s"try to execute ddl:${CanalEntryTransHelper.headerToJson(entry.getHeader)},id:$syncTaskId")
    val ddlSql = CanalEntryTransUtil.parseStoreValue(entry)(syncTaskId).getSql
    val (sdaDbName, sdaTableName) = rule.getMappingName(entry.getHeader.getSchemaName, entry.getHeader.getTableName)

  }
}

object SdaMysqlBinlogInOrderDirectFetcher {
  val name: String = SdaMysqlBinlogInOrderDirectFetcher.getClass.getName.stripSuffix("$")

  def props(taskManager: Mysql2MysqlTaskInfoManager, downStream: ActorRef): Props = Props(new SdaMysqlBinlogInOrderDirectFetcher(taskManager, downStream))
}


