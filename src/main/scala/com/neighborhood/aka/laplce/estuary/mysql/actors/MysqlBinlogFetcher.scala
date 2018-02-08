package com.neighborhood.aka.laplce.estuary.mysql.actors

import java.io.IOException
import java.nio.ByteBuffer
import java.util

import akka.actor.SupervisorStrategy.{Escalate, Restart, Resume, Stop}
import akka.actor.{Actor, ActorRef, OneForOneStrategy}
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager
import com.alibaba.otter.canal.parse.exception.CanalParseException
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplce.estuary.core.lifecycle.{SourceDataFetcher, Status}
import com.neighborhood.aka.laplce.estuary.mysql.{Mysql2KafkaTaskInfoManager, MysqlBinlogParser}
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent
import com.taobao.tddl.dbsync.binlog.{LogContext, LogDecoder, LogEvent, LogPosition}
import org.apache.commons.lang.StringUtils

import scala.annotation.tailrec

/**
  * Created by john_liu on 2018/2/5.
  */
class MysqlBinlogFetcher(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager, binlogParser: MysqlBinlogParser, binlogEventBatcher: ActorRef) extends Actor with SourceDataFetcher {
  override var errorCountThreshold: Int = 3
  override var errorCount: Int = 0
  val slaveId = mysql2KafkaTaskInfoManager.slaveId
  val mysqlConnection: Option[MysqlConnection] = Option(mysql2KafkaTaskInfoManager.mysqlConnection)
  val logPositionHandler = mysql2KafkaTaskInfoManager.logPositionHandler
  var entryPosition: Option[EntryPosition] = None

  var binlogChecksum = 0
  var necessary = mysql2KafkaTaskInfoManager.taskInfo.isProfiling

  var fetcher: DirectLogFetcher = null
  var logContext: LogContext = null
  var decoder: LogDecoder = null

  //offline
  override def receive: Receive = {

    case FetcherMessage(msg) => {

    }
    case SyncControllerMessage(msg) => {
      msg match {
        case "stop" => {

        }
        case "start" => {
          entryPosition = Option(logPositionHandler.findEndPosition(mysqlConnection.get))
          if (entryPosition.isDefined) {
            context.become(online)
            self ! FetcherMessage("start")
          } else {
            //todo logstash
          }
        }
      }
    }
  }

  def online: Receive = {
    case FetcherMessage(msg) => {
      msg match {
        case "start" => {
          //将状态置为BUSY
          mysql2KafkaTaskInfoManager.fetcherStatus = Status.BUSY
          val startPosition = entryPosition.get
          if (StringUtils.isEmpty(startPosition.getJournalName) && Option(startPosition.getTimestamp).isEmpty) {} else {
            dump(startPosition.getJournalName, startPosition.getPosition)
          }
        }
        case "fetch" => {
          if (fetcher.fetch()) {
            fetchOne
            self ! FetcherMessage("fetch")
          } else {
            self ! FetcherMessage("afterFetch")
          }
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
    updateSettings
    val connector = mysqlConnection.get.getConnector
    fetcher = new DirectLogFetcher(connector.getReceiveBufferSize)
    fetcher.start(connector.getChannel)
    decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT)
    logContext = new LogContext
    logContext.setLogPosition(new LogPosition(binlogFileName))
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

  def fetchOne = {
    val event = decoder.decode(fetcher, logContext)
    val entry = parseAndProfilingIfNecessary(event, false)
    if (entry.isDefined) {
      //todo logstash
      binlogEventBatcher ! entry.get
    } else {
      //todo 出现null值处理
    }
  }

  def updateSettings: Unit = {
    val settings = List(
      "set wait_timeout=9999999",
      "set net_write_timeout=1800",
      "set net_read_timeout=1800",
      "set names 'binary'",
      "set @master_binlog_checksum= @@global.binlog_checksum"
    )
    settings.map(mysqlConnection.get.update(_))

  }

  private def loadBinlogChecksum(): Unit = {
    var rs: ResultSetPacket = null
    try
      rs = mysqlConnection.get.query("select @master_binlog_checksum")
    catch {
      case e: IOException =>
        throw new CanalParseException(e)
    }
    val columnValues: util.List[String] = rs.getFieldValues
    binlogChecksum = if (columnValues != null && columnValues.size >= 1 && columnValues.get(0).toUpperCase == "CRC32") LogEvent.BINLOG_CHECKSUM_ALG_CRC32
    else LogEvent.BINLOG_CHECKSUM_ALG_OFF
  }

  @throws[IOException]
  private def sendBinlogDump(binlogfilename: String, binlogPosition: Long) = {
    val binlogDumpCmd = new BinlogDumpCommandPacket
    binlogDumpCmd.binlogFileName = binlogfilename
    binlogDumpCmd.binlogPosition = binlogPosition
    binlogDumpCmd.slaveServerId = this.slaveId
    val cmdBody = binlogDumpCmd.toBytes
    //todo logstash
    val binlogDumpHeader = new HeaderPacket
    binlogDumpHeader.setPacketBodyLength(cmdBody.length)
    binlogDumpHeader.setPacketSequenceNumber(0x00.toByte)
    PacketManager.write(mysqlConnection.get.getConnector.getChannel, Array[ByteBuffer](ByteBuffer.wrap(binlogDumpHeader.toBytes), ByteBuffer.wrap(cmdBody)))
    mysqlConnection.get.getConnector.setDumping(true)
  }

  def parseAndProfilingIfNecessary(event: LogEvent, necessary: Boolean): Option[CanalEntry.Entry] = {

    if (necessary) {
      //todo
    }
    binlogParser.parse(Option(event))
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: Exception => Restart
      case _ => Escalate
    }
  }
  def processError(e:Throwable) = {
    errorCount += 1
    if(isCrashed){
        throw
    }
  }
}
