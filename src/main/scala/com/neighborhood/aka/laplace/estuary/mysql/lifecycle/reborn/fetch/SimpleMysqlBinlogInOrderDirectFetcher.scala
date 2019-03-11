package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

import akka.actor.{ActorRef, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.schema.Parser
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp
import com.neighborhood.aka.laplace.estuary.mysql.task.mysql.Mysql2MysqlTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.utils.CanalEntryTransUtil

/**
  * Created by john_liu on 2019/1/16.
  *
  * 直接执行ddl的SimpleFetcher
  *
  * @author neighborhood.aka.laplace
  */
final class SimpleMysqlBinlogInOrderDirectFetcher(
                                                   override val taskManager: Mysql2MysqlTaskInfoManager,
                                                   override val downStream: ActorRef) extends MysqlBinlogInOrderDirectFetcher(taskManager, downStream) {

  lazy val sink: MysqlSinkFunc = taskManager.sink

  lazy val schemaHolder = taskManager.sinkMysqlTableSchemaHolder

  override protected def executeDdl(entry: CanalEntry.Entry): Unit = {
    val ddlSql = CanalEntryTransUtil.parseStoreValue(entry)(syncTaskId).getSql
    val safeDdlSql = List(s"use ${entry.getHeader.getSchemaName};", ddlSql)
    taskManager.wait4TheSameCount() //必须要等到same count
    //todo
    if (!sink.isTerminated) sink.insertBatchSql(safeDdlSql) //出现异常的话一定要暴露出来
    if (schemaComponentIsOn) schemaHolder.updateTableMeta(Parser.parse(ddlSql, entry.getHeader.getSchemaName).head)

  }

  /**
    * ********************* Actor生命周期 *******************
    */

}

object SimpleMysqlBinlogInOrderDirectFetcher {
  val name: String = SimpleMysqlBinlogInOrderDirectFetcher.getClass.getName.stripSuffix("$")

  def props(taskManager: Mysql2MysqlTaskInfoManager,
            downStream: ActorRef): Props = Props(new SimpleMysqlBinlogInOrderDirectFetcher(taskManager, downStream))
}
