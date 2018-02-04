package com.neighborhood.aka.laplce.estuary.mysql.actors

import java.io.IOException

import akka.actor.{Actor, Props}
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter
import com.alibaba.otter.canal.parse.CanalEventParser
import com.alibaba.otter.canal.parse.exception.CanalParseException
import com.alibaba.otter.canal.parse.inbound.ErosaConnection
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection.{BinlogFormat, BinlogImage}
import com.alibaba.otter.canal.parse.inbound.mysql.{AbstractMysqlEventParser, MysqlConnection}
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplce.estuary.mysql.MysqlTaskInfoResourceManager

/**
  * Created by john_liu on 2018/2/1.
  */

class DSPMySqlEventParser[EVENT](rm: MysqlTaskInfoResourceManager) extends AbstractMysqlEventParser with CanalEventParser[EVENT] with Actor {
  //资源管理器，一次同步任务所有的resource都由resourceManager负责
  val resourceManager = rm
  //配置
  val config = context.system.settings.config
  //binlog解析器
  val binlogParser = buildParser()
  //canal的mysqlConnection
  val mysqlConnection: MysqlConnection = buildErosaConnection()
  // 支持的binlogFormat,如果设置会执行强校验
  lazy val supportBinlogFormats = Option(config
    .getString("common.binlog.formats"))
    .map {
      formatsStr =>
        formatsStr
          .split(",")
          .map {
            formatStr =>
              formatsStr match {
                case "ROW" => BinlogFormat.ROW
                case "STATEMENT" => BinlogFormat.STATEMENT
                case "MIXED" => BinlogFormat.MIXED
              }
          }
    }
  //支持的binlogImage，如果设置会执行强校验
  lazy val supportBinlogImages = Option(config
    .getString(s"common.binlog.images")
  )
    .map {
      binlogImagesStr =>
        binlogImagesStr.split(",")
          .map {
            binlogImageStr =>
              binlogImageStr match {
                case "FULL" => BinlogImage.FULL
                case "MINIMAL" => BinlogImage.MINIMAL
                case "NOBLOB" => BinlogImage.NOBLOB
              }
          }
    }
  //val masterFirstPosition =

  //offline 状态
  override def receive: Receive = {
    case "start" => {
      context.become(online)
      preDump(mysqlConnection)
      mysqlConnection.connect()
      //todo 获取最后的位置信息
      //重新连接，因为在找position过程中可能有状态，需要断开后重建
      mysqlConnection.reconnect()

    }
    case ListenerMessage(msg) => {
      msg match {
        case _ => {}
      }
    }
  }

  def online: Receive = {
    case ListenerMessage(msg) => {
      msg match {
        case "connection" => {
          //todo logStash
          sender() ! mysqlConnection
          sender() ! EventParserMessage("start")
        }
        case "reconnect" => {
          mysqlConnection.reconnect()
        }
      }
    }
  }



  /**
    * @return 返回MysqlConnection 而不是 其父类ErosaConnection
    *
    */
  override def buildErosaConnection(): MysqlConnection = {
    resourceManager.mysqlConnection
  }

  override def buildParser(): DSPBinlogParser = {
    val convert = resourceManager.binlogParser
    if (eventFilter != null && eventFilter.isInstanceOf[AviaterRegexFilter]) convert.setNameFilter(eventFilter.asInstanceOf[AviaterRegexFilter])

    if (eventBlackFilter != null && eventBlackFilter.isInstanceOf[AviaterRegexFilter]) convert.setNameBlackFilter(eventBlackFilter.asInstanceOf[AviaterRegexFilter])

    convert.setCharset(connectionCharset)
    convert.setFilterQueryDcl(filterQueryDcl)
    convert.setFilterQueryDml(filterQueryDml)
    convert.setFilterQueryDdl(filterQueryDdl)
    convert.setFilterRows(filterRows)
    convert.setFilterTableError(filterTableError)
    return convert
  }

  /**
    * Canal中，preDump中做了binlogFormat/binlogImage的校验
    * 这里暂时忽略，可以再以后有必要的时候进行添加
    *
    */
  override def preDump(connection: ErosaConnection): Unit = {
    //设置tableMetaCache
    binlogParser.setTableMetaCache(resourceManager.tableMetaCache)
  }


  //Actor生命周期
  //每次启动都会调用，在构造器之后调用
  override def preStart(): Unit = {
    //todo logstash
    //初始化HeartBeatsListener
    context.actorOf(Props(classOf[MysqlConnectionListenerActor]), "heartBeatsListener")
  }

  //正常关闭时会调用，关闭资源
  override def postStop(): Unit = {
    //todo logstash
    mysqlConnection.disconnect()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    //todo logstash
    //默认的话是会调用postStop，preRestart可以保存当前状态
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    //todo logstash
    //可以恢复之前的状态，默认会调用
    super.postRestart(reason)
  }


}
