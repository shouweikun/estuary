package com.neighborhood.aka.laplce.estuary.mysql.actors

import java.io.IOException
import java.nio.ByteBuffer
import java.util

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager
import com.alibaba.otter.canal.parse.exception.{CanalParseException, TableIdNotFoundException}
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.{DirectLogFetcher, TableMetaCache}
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplce.estuary.core.lifecycle._
import com.neighborhood.aka.laplce.estuary.mysql.{Mysql2KafkaTaskInfoManager, MysqlBinlogParser}
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent
import com.taobao.tddl.dbsync.binlog.{LogContext, LogDecoder, LogEvent, LogPosition}
import org.I0Itec.zkclient.exception.ZkTimeoutException
import org.apache.commons.lang.StringUtils

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by john_liu on 2018/2/5.
  *
  * @todo 将fetcher和actor解耦
  */

class MysqlBinlogFetcher(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager, binlogEventBatcher: ActorRef) extends Actor with SourceDataFetcher {
  /**
    * binlogParser 解析binlog
    */
  lazy val binlogParser: MysqlBinlogParser = mysql2KafkaTaskInfoManager.binlogParser
  /**
    * 重试机制
    */
  override var errorCountThreshold: Int = 3
  override var errorCount: Int = 0
  /**
    * 模拟的从库id
    */
  val slaveId = mysql2KafkaTaskInfoManager.slaveId
  /**
    * mysql链接
    */
  val mysqlConnection: Option[MysqlConnection] = Option(mysql2KafkaTaskInfoManager.mysqlConnection)
  /**
    * 寻址处理器
    */
  val logPositionHandler = mysql2KafkaTaskInfoManager.logPositionHandler
  /**
    * 暂存的entryPosition
    */
  var entryPosition: Option[EntryPosition] = None
  /**
    * mysql的checksum校验机制
    */
  var binlogChecksum = 0
  /**
    * 是否记录耗时
    */
  var necessary = mysql2KafkaTaskInfoManager.taskInfo.isProfiling
  /**
    * 数据fetch用
    */
  var fetcher: DirectLogFetcher = null
  var logContext: LogContext = null
  var decoder: LogDecoder = null

  //offline
  override def receive: Receive = {

    case FetcherMessage(msg) => {
      msg match {
        case "restart" => {
          switch2Restarting
          self ! SyncControllerMessage("start")
        }
        case str:String => {
          println(s"fetcher offline  unhandled message:$str")
        }
      }

    }
    case SyncControllerMessage(msg) => {
      msg match {
        case "stop" => {
          context.become(receive)
          switch2Offline
        }
        case "start" => {
          try {
            entryPosition = Option(logPositionHandler.findStartPosition(mysqlConnection.get)(errorCount > 0))
            if (entryPosition.isDefined) {
              //寻找完后必须reconnect一下
              mysqlConnection.get.synchronized {
                mysqlConnection.get.reconnect
              }
              context.become(online)
              self ! FetcherMessage("start")
            } else {
              throw new Exception("entryPosition is null")
            }
          }
          catch {
            case e: Exception => processError(e, SyncControllerMessage("start"))
          }

        }
      }
    }
  }

  def online: Receive = {
    case FetcherMessage(msg) => {
      msg match {
        case "start" => {
          switch2Busy
          self ! FetcherMessage("predump")
        }
        case "predump" => {
          preDump(mysqlConnection.get)
          mysqlConnection.get.connect()
          val startPosition = entryPosition.get
          throw new Exception("故意的")
          try {
            if (StringUtils.isEmpty(startPosition.getJournalName) && Option(startPosition.getTimestamp).isEmpty) {} else {
              dump(startPosition.getJournalName, startPosition.getPosition)

            }
            self ! FetcherMessage("fetch")
          } catch {
            case e: Exception => processError(e, FetcherMessage("start"))
          }
        }
        case "fetch" => {
          try {
            if (fetcher.fetch()) {
              fetchOne
              self ! FetcherMessage("fetch")
            } else {
              switch2Free
            }
          } catch {
            case e: TableIdNotFoundException => {
              entryPosition = Option(logPositionHandler.findStartPositionWithinTransaction(mysqlConnection.get)(errorCount > 0))
              self ! FetcherMessage("start")
            }
            case e: Exception => processError(e, FetcherMessage("fetch"))
          }
        }
        case "restart" => {
          context.become(receive)
          self ! FetcherMessage("restart")
        }
        case str: String => {
          println(s"fetcher online unhandled command:$str")
        }
      }
    }
    case SyncControllerMessage(msg) => {

    }
  }

  /**
    * @param binlogFileName binlog文件名称
    * @param binlogPosition binlogEntry位置
    *
    */
  def dump(binlogFileName: String, binlogPosition: Long) = {
    updateSettings(mysqlConnection.get)
    loadBinlogChecksum(mysqlConnection.get)
    val connector = mysqlConnection.get.getConnector
    fetcher = new DirectLogFetcher(connector.getReceiveBufferSize)
    fetcher.start(connector.getChannel)
    decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT)
    logContext = new LogContext
    logContext.setLogPosition(new LogPosition(binlogFileName,binlogPosition))
    logContext.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum))
  }

  @deprecated("use `fetchOne`")
  @tailrec
  final def loopFetchAll: Unit = {
    if (fetcher.fetch()) {
      fetchOne
      loopFetchAll
    }
  }

  /**
    * 从连接中取数据
    */
  def fetchOne = {
    val event = decoder.decode(fetcher, logContext)
    val entry = binlogParser.parseAndProfilingIfNecessary(event, false)
    if (entry.isDefined) {
      //todo logStash
      //todo logPosition如何获取
      binlogEventBatcher ! entry.get
    } else {
      throw new Exception("the fetched data is null")
    }
  }

  /**
    * 从binlog拉取数据之前的设置
    */
  def updateSettings(mysqlConnection: MysqlConnection): Unit = {
    val settings = List(
      "set wait_timeout=9999999",
      "set net_write_timeout=1800",
      "set net_read_timeout=1800",
      "set names 'binary'",
      "set @master_binlog_checksum= @@global.binlog_checksum"
    )
    settings.map(mysqlConnection.update(_))

  }

  /**
    * 加载mysql的binlogChecksum机制
    *
    */
  private def loadBinlogChecksum(mysqlConnection: MysqlConnection): Unit = {
    var rs: ResultSetPacket = null
    try
      rs = mysqlConnection.query("select @master_binlog_checksum")
    catch {
      case e: IOException =>
        throw new CanalParseException(e)
    }
    val columnValues: util.List[String] = rs.getFieldValues
    binlogChecksum = if (columnValues != null && columnValues.size >= 1 && columnValues.get(0).toUpperCase == "CRC32") LogEvent.BINLOG_CHECKSUM_ALG_CRC32
    else LogEvent.BINLOG_CHECKSUM_ALG_OFF
  }

  /**
    * 准备dump数据
    */
  @throws[IOException]
  private def sendBinlogDump(binlogfilename: String, binlogPosition: Long)(mysqlConnection: MysqlConnection) = {
    val binlogDumpCmd = new BinlogDumpCommandPacket
    binlogDumpCmd.binlogFileName = binlogfilename
    binlogDumpCmd.binlogPosition = binlogPosition
    binlogDumpCmd.slaveServerId = this.slaveId
    val cmdBody = binlogDumpCmd.toBytes
    //todo logstash
    val binlogDumpHeader = new HeaderPacket
    binlogDumpHeader.setPacketBodyLength(cmdBody.length)
    binlogDumpHeader.setPacketSequenceNumber(0x00.toByte)
    PacketManager.write(mysqlConnection.getConnector.getChannel, Array[ByteBuffer](ByteBuffer.wrap(binlogDumpHeader.toBytes), ByteBuffer.wrap(cmdBody)))
    mysqlConnection.getConnector.setDumping(true)
  }

  /**
    * Canal中，preDump中做了binlogFormat/binlogImage的校验
    * 这里暂时忽略，可以再以后有必要的时候进行添加
    *
    */
  def preDump(mysqlConnection: MysqlConnection): Unit = {
    //设置tableMetaCache
    val metaConnection = mysqlConnection.fork()
    metaConnection.connect()
    val tableMetaCache: TableMetaCache = new TableMetaCache(metaConnection)
    binlogParser.setTableMetaCache(tableMetaCache)
  }

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: WorkerMessage): Unit = {
    //todo 记录log
    errorCount += 1
    if (isCrashed) {
      switch2Error
      errorCount = 0
      println(message.msg)
      throw new Exception("fetching data failure for 3 times")
    } else {
      self ! message
    }
  }

  /**
    * ********************* 状态变化 *******************
    */
  private def switch2Offline = {
    mysql2KafkaTaskInfoManager.fetcherStatus = Status.OFFLINE
  }

  private def switch2Busy = {
    mysql2KafkaTaskInfoManager.fetcherStatus = Status.BUSY
  }

  private def switch2Error = {
    mysql2KafkaTaskInfoManager.fetcherStatus = Status.ERROR
  }

  private def switch2Free = {
    mysql2KafkaTaskInfoManager.fetcherStatus = Status.FREE
  }

  private def switch2Restarting = {
    mysql2KafkaTaskInfoManager.fetcherStatus = Status.RESTARTING
  }

  /**
    * ********************* Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    //状态置为offline
    switch2Offline
    context.system.scheduler.scheduleOnce(1 minutes)(self ! FetcherMessage("restart"))

  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
  }


  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {

    context.become(receive)
    switch2Offline
    super.preRestart(reason, message)
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: ZkTimeoutException => {
        Restart
        //todo log
      }
      case e: Exception => Restart
      case error:Error => Restart
      case _ => Restart
    }
  }

}
object MysqlBinlogFetcher {
  def props(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,binlogEventBatcher: ActorRef) :Props= {
    Props(new MysqlBinlogFetcher(mysql2KafkaTaskInfoManager,binlogEventBatcher))
  }
}
