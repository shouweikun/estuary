package com.neighborhood.aka.laplce.estuary.mysql.actors

import java.io.IOException
import java.nio.ByteBuffer
import java.util
import java.util.List

import akka.actor.{Actor, ActorRef, Props}
import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager
import com.alibaba.otter.canal.parse.exception.CanalParseException
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent
import com.taobao.tddl.dbsync.binlog.{LogContext, LogDecoder, LogEvent, LogPosition}
import org.apache.commons.lang.StringUtils
/**
  * Created by john_liu on 2018/2/5.
  */
class DSPMysqlBinlogFetcher(conn: MysqlConnection = null,slaveId:Long,binlogEventBatcher:ActorRef) extends Actor {
  var entryPosition: Option[EntryPosition] = None
  var mysqlConnection: Option[MysqlConnection] = Option(conn)
  var binlogChecksum = 0



  //offline
  override def receive: Receive = {
    case ep: EntryPosition => {
      entryPosition = Option(ep)
      if (entryPosition.isDefined) {
        context.become(online)
        self ! "start"
      }
    }
    case conn: MysqlConnection => {
      mysqlConnection = Option(conn)
    }

    case FetcherMessage(msg) => {

    }
    case EventParserMessage(msg) => {

    }
  }

  def online: Receive = {
    case FetcherMessage(msg) => {
      msg match {
        case "start" => {
          val startPosition = entryPosition.get
          if (StringUtils.isEmpty(startPosition.getJournalName) && Option(startPosition.getTimestamp).isEmpty) {}
        }
      }
    }
    case EventParserMessage(msg) => {

    }
  }

  def dump(binlogFileName: String, binlogPosition: Long) = {
    updateSettings
    val connector = mysqlConnection.get.getConnector
    val fetcher = new DirectLogFetcher(connector.getReceiveBufferSize)
    fetcher.start(connector.getChannel)
    val decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT)
    val logContext = new LogContext
    logContext.setLogPosition(new LogPosition(binlogFileName))
    logContext.setFormatDescription(new FormatDescriptionLogEvent(4, binlogChecksum))


  }

  def updateSettings :Unit = {
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
    if (columnValues != null && columnValues.size >= 1 && columnValues.get(0).toUpperCase == "CRC32") binlogChecksum = LogEvent.BINLOG_CHECKSUM_ALG_CRC32
    else binlogChecksum = LogEvent.BINLOG_CHECKSUM_ALG_OFF
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
}
