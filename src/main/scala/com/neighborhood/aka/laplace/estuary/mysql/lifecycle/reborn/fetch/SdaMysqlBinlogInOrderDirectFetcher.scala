package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

import akka.actor.{ActorRef, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.EventType
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2MysqlTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.utils.CanalEntryTransUtil

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

  lazy val sink: MysqlSinkFunc = taskManager.sink

  lazy val rule: Map[String, String] = taskManager.tableMappingRule

  private lazy val ALTER_PATTERN = """^\s*[a/A][l/L][t/T][e/E][r/R]\s+[T/t][a/A][b/B][l/L][e/E]\s*(\w+).*""".r
  A

  override protected def executeDdl(entry: CanalEntry.Entry): Unit = {
    val ddlSql = CanalEntryTransUtil.parseStoreValue(entry)(syncTaskId).getSql
    val sdaDbAndTableName: String = rule(s"${entry.getHeader.getSchemaName}.${entry.getHeader.getTableName}")
    lazy val handleAlter: String = ALTER_PATTERN.replaceFirstIn(ddlSql, sdaDbAndTableName)
    taskManager.wait4TheSameCount() //必须要等到same count
    //    val ddlResult = SimpleDdlParser.parse(ddlSql, entry.getHeader.getSchemaName)
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


