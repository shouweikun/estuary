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
import com.neighborhood.aka.laplce.estuary.mysql.{Mysql2KafkaTaskInfoManager, MysqlTaskInfoResourceManager}
import org.apache.commons.lang.StringUtils

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Created by john_liu on 2018/2/1.
  */

class DSPMySqlEventParser[EVENT](rm: Mysql2KafkaTaskInfoManager) extends AbstractMysqlEventParser with CanalEventParser[EVENT] with Actor {
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
  val logPositionFinder = resourceManager.logPositionFinder
  val dumpErrorCountThreshold = 3
  var dumpErrorCount = 0
  val retryBackoff = Option(config.getInt("common.retry.backoff")).getOrElse(30)
  var entryPosition: EntryPosition = null


  //offline 状态
  override def receive: Receive = {
    case "start" => {
      context.become(online)
      self ! EventParserMessage("predump")

    }
    case ListenerMessage(msg) => {
      msg match {
        case "connection" => {
          //todo logStash
          sender() ! mysqlConnection
        }
        case "reconnect" => {
          mysqlConnection.reconnect()
        }
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
    case EventParserMessage(msg) => {
      msg match {
        case "predump" => {
          preDump(mysqlConnection)
          mysqlConnection.connect()
          self ! EventParserMessage("findLog")


        }
        case "findLog" => {
          //寻找log

          Try(entryPosition = logPositionFinder.findStartPosition(mysqlConnection)(retryCountdown)) match {
            case Success(x) => {
              //重新连接，因为在找position过程中可能有状态，需要断开后重建
              //由于mysqlConnection同时也被listener持有，所以synchronized
              //这个设计很蠢-_-!
              mysqlConnection.synchronized {
                mysqlConnection.reconnect
              }
              self ! EventParserMessage("dump")
            }
            case Failure(e) => {
              processDumpError(e)
              if (retryCountdown) {
                self ! EventParserMessage("findLog")
              }
              context.become(retryBack)
              context.system.scheduler.scheduleOnce(retryBackoff minutes)(self ! "restart")
            }
          }

        }
        case "dump" => {

        }
      }
    }
  }

  def retryBack: Receive = {
    case "restart" => {
        context.become(receive)
        self ! "start"
    }
  }

  /**
    * @return 返回MysqlConnection 而不是 其父类ErosaConnection
    *
    */
  override def buildErosaConnection(): MysqlConnection = {
    resourceManager.mysqlConnection
  }

  def retryCountdown: Boolean = {
    dumpErrorCountThreshold >= 0 && dumpErrorCount > dumpErrorCountThreshold
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


  /**
    ***************** Actor生命周期 *******************
   */

  /**
    * 每次启动都会调用，在构造器之后调用
    * 1.初始化HeartBeatsListener
    * 2.初始化binlogSinker
    * 3.初始化binlogEventBatcher
    * 4.初始化binlogFetcher
    */
  override def preStart(): Unit = {
    //todo logstash
    //初始化HeartBeatsListener
    context.actorOf(Props(classOf[MysqlConnectionListenerActor],resourceManager), "heartBeatsListener")
    //初始化binlogSinker
    //如果并行打开使用并行sinker
   val binlogSinker = if(resourceManager.taskInfo.isTransactional){

    }else{
     //不是然使用transaction式
      context.actorOf(Props(classOf[BinlogTransactionBufferSinker],resourceManager),"binlogSinker")
    }
    //初始化binlogEventBatcher
   val binlogEventBatcher = context.actorOf(Props(classOf[BinlogEventBatcher],resourceManager,binlogSinker)
    ,"binlogEventBatcher")
    //初始化binlogFetcher
    context.actorOf(Props(classOf[DSPMysqlBinlogFetcher],resourceManager,binlogEventBatcher),"binlogFetcher")
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

  override protected def processDumpError(e: Throwable): Unit = {
    if (e.isInstanceOf[IOException]) {
      val message = e.getMessage
      if (StringUtils.contains(message, "errno = 1236")) { // 1236 errorCode代表ER_MASTER_FATAL_ERROR_READING_BINLOG
        dumpErrorCount += 1
      }
    }
  }
}
