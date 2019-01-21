package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

import akka.actor.{ActorRef, Props}
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.SimpleDdlParser
import com.alibaba.otter.canal.protocol.CanalEntry
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

  override protected def executeDdl(entry: CanalEntry.Entry): Unit = {
    val ddlSql = CanalEntryTransUtil.parseStoreValue(entry)(syncTaskId).getSql

    taskManager.wait4TheSameCount() //必须要等到same count
    val ddlResult = SimpleDdlParser.parse(ddlSql, entry.getHeader.getSchemaName)
    //todo
    if (sink.isTerminated) sink.insertSql(ddlSql) //出现异常的话一定要暴露出来
  }
}

object SdaMysqlBinlogInOrderDirectFetcher {
  val name: String = SdaMysqlBinlogInOrderDirectFetcher.getClass.getName.stripSuffix("$")

  def props(taskManager: Mysql2MysqlTaskInfoManager, downStream: ActorRef): Props = Props(new SdaMysqlBinlogInOrderDirectFetcher(taskManager, downStream))
}


