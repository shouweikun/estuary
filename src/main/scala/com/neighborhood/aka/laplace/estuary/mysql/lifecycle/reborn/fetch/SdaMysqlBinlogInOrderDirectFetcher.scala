package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

import akka.actor.{ActorRef, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.core.lifecycle.FetcherMessage
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.record.MysqlBinlogInOrderRecorderCommand.MysqlBinlogInOrderRecorderSaveLatestPosition
import com.neighborhood.aka.laplace.estuary.mysql.schema.{Parser, SdaSchemaMappingRule}
import com.neighborhood.aka.laplace.estuary.mysql.task.mysql.Mysql2MysqlTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.utils.{CanalEntryTransHelper, CanalEntryTransUtil}

import scala.util.{Failure, Success, Try}

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

  private lazy val schemaHolder = taskManager.sinkMysqlTableSchemaHolder

  private lazy val isSchemaComponentOn = taskManager.schemaComponentIsOn

  /**
    * SDA专用DDL处理
    *
    * 不论对于何种ddl,都会尝试发起SDA改名，如果未能找到,警告，并使用原始名称进行执行
    * 1. 获取ddl
    * 2.改名
    * 3.更新Schema
    * 4.转换成DDl Sql String
    * 5.执行
    *
    * @note 执行失败是被忽略的，这么做的理由是让失败暴露在数据写入的部分，这样防止一些可以忽略的执行失败阻碍程序正常运行流程
    * @param entry
    * @author neighborhood.aka.laplace
    */
  override protected def executeDdl(entry: CanalEntry.Entry): Unit = {
    assert(CanalEntryTransUtil.isDdl(entry)) //确认是ddl
    import com.neighborhood.aka.laplace.estuary.mysql.schema.Parser.SchemaChangeToDdlSqlSyntax
    log.info(s"try to execute ddl:${CanalEntryTransHelper.headerToJson(entry.getHeader)},id:$syncTaskId")
    val ddlSql = CanalEntryTransUtil.parseStoreValue(entry)(syncTaskId).getSql
    log.info(s"ddl sql is $ddlSql,id:$syncTaskId")
    //    val sdaDbName = rule.getDatabaseMappingName(entry.getHeader.getSchemaName).get
    val schemaChange = Parser.parseAndReplace(ddlSql, (entry.getHeader.getSchemaName), rule) //只会是一条
    if (isSchemaComponentOn) schemaHolder.updateTableMeta(schemaChange, sink) //更新Schema
    val finalDdl = schemaChange.toDdlSqlWithSink(Option(sink))
    taskManager.wait4TheSameCount() //等待执行完毕
    log.info(s"start to execute sda finalDdl:$ddlSql,finalDdl:$finalDdl,id:$syncTaskId")
    val execution = Try(finalDdl.map(sink.insertSql(_)))
    execution match {
      case Success(_) => log.info(
        s"""
           |
           |
           |
           |
           |
           |
           |
           |
           |
           |ddl:$finalDdl executing success,id:$syncTaskId
           |
           |
           |
           |
           |
           |
           |
           |
         """.stripMargin)
      case Failure(e) => log.error(
        s"""
           |
           |
           |
           |
           |
           |
           |ddl:$finalDdl executing failure,e:$e,message:${e.getMessage},id:$syncTaskId
           |
           |
           |
           |
           |
           |
         """.stripMargin)
    }
    if (execution.isSuccess) positionRecorder.fold(log.warning(s"can not find position recorder when sending save latest saving offset command,id:$syncTaskId"))(ref => ref ! FetcherMessage(MysqlBinlogInOrderRecorderSaveLatestPosition)) //发送保存命令
  }
}

object SdaMysqlBinlogInOrderDirectFetcher {
  val name: String = SdaMysqlBinlogInOrderDirectFetcher.getClass.getName.stripSuffix("$")

  def props(taskManager: Mysql2MysqlTaskInfoManager, downStream: ActorRef): Props = Props(new SdaMysqlBinlogInOrderDirectFetcher(taskManager, downStream))
}


