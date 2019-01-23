package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

import akka.actor.{ActorRef, Props}
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.SimpleDdlParser
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.EventType
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2MysqlTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.utils.{CanalEntryJsonHelper, CanalEntryTransUtil}

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

  private lazy val rule: Map[String, String] = taskManager.tableMappingRule

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
    log.info(s"try to execute ddl:${CanalEntryJsonHelper.headerToJson(entry.getHeader)},id:$syncTaskId")
    val ddlSql = CanalEntryTransUtil.parseStoreValue(entry)(syncTaskId).getSql
    val sdaDbAndTableName: String = rule.getOrElse(s"${entry.getHeader.getSchemaName}.${entry.getHeader.getTableName}", s"${entry.getHeader.getSchemaName}.${entry.getHeader.getTableName}") //注意，如果在
    lazy val handleAlter: String = ???
    taskManager.wait4TheSameCount() //必须要等到same count
    val sdaDdlSql = entry.getHeader.getEventType match {
      case EventType.ALTER => handleAlter
      case _ => ???
    }
    if (!sink.isTerminated) sink.insertSql(sdaDdlSql) //出现异常的话一定要暴露出来
  }
}

object SdaMysqlBinlogInOrderDirectFetcher {
  val name: String = SdaMysqlBinlogInOrderDirectFetcher.getClass.getName.stripSuffix("$")

  def props(taskManager: Mysql2MysqlTaskInfoManager, downStream: ActorRef): Props = Props(new SdaMysqlBinlogInOrderDirectFetcher(taskManager, downStream))
}


