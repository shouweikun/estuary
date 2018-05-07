package com.neighborhood.aka.laplace.estuary.mysql.source

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util

import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager
import com.alibaba.otter.canal.parse.driver.mysql.{MysqlConnector, MysqlQueryExecutor, MysqlUpdateExecutor}
import com.alibaba.otter.canal.parse.exception.CanalParseException
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection.{BinlogFormat, BinlogImage}
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.{DirectLogFetcher, TableMetaCache}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import com.neighborhood.aka.laplace.estuary.mysql.utils.MysqlBinlogParser
import com.taobao.tddl.dbsync.binlog.event.FormatDescriptionLogEvent
import com.taobao.tddl.dbsync.binlog.{LogContext, LogDecoder, LogEvent, LogPosition}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

/**
  * Created by john_liu on 2018/3/21.
  *
  * @todo
  */

class MysqlConnection(
                       private val charset: Charset = Charset.forName("UTF-8"), //字符集
                       private val charserNum: Byte = 33.toByte, //字符集序号
                       private val binlogFormat: BinlogFormat = null,
                       private val slaveId: Long = System.currentTimeMillis(),
                       private val binlogImage: BinlogImage = null,
                       private val address: InetSocketAddress,
                       private val receiveBufferSize: Int = 16 * 1024 * 1024,
                       private val sendBufferSize: Int = 16 * 1024,
                       private val username: String,
                       private val password: String,
                       private val database: String = "retl"
                     ) extends DataSourceConnection {


  val logger = LoggerFactory.getLogger(classOf[MysqlConnection])
  /**
    * Canal 的Connector 主要是用于建立tcp连接，拉取数据
    */
  private lazy val connector: MysqlConnector = {
    lazy val re = new MysqlConnector(address, username, password, charserNum, database)
    re.setReceiveBufferSize(receiveBufferSize)
    re.setSendBufferSize(sendBufferSize)
    re
  }
  /**
    * mysql的checksum校验机制
    */

  private var binlogChecksum: Int = 0
  /**
    * 数据fetch用
    */
  var fetcher: DirectLogFetcher = null
  var logContext: LogContext = null
  var decoder: LogDecoder = null

  /**
    * 数据seek用
    */
  var fetcher4Seek: DirectLogFetcher = null
  var logContext4Seek: LogContext = null
  var decoder4Seek: LogDecoder = null

  /**
    * 数据seek模式标志
    */
  var seekFlag: Boolean = false

  /**
    * 连接
    */
  override def connect(): Unit = connector.connect()

  /**
    * 重连
    */
  override def reconnect(): Unit = connector.reconnect()

  /**
    * 断开
    */
  override def disconnect(): Unit = connector.disconnect()

  /**
    * 是否连接
    */
  override def isConnected: Boolean = connector.isConnected

  /**
    *
    * @param cmd 查询sql命令
    * @throws IOException
    * @return 数据包
    */
  @throws[IOException]
  def query(cmd: String): ResultSetPacket = {
    val exector = new MysqlQueryExecutor(connector)
    exector.query(cmd)
  }

  /**
    *
    * @param cmd update sql 命令
    * @throws IOException
    */
  @throws[IOException]
  def update(cmd: String): Unit = {
    val exector = new MysqlUpdateExecutor(connector)
    exector.update(cmd)
  }

  /**
    * 创造出一个新的链接
    *
    * @return MysqlConnection
    */
  def fork: MysqlConnection = {
    new MysqlConnection(
      charset,
      charserNum,
      binlogFormat,
      System.currentTimeMillis(),
      binlogImage,
      address,
      receiveBufferSize,
      sendBufferSize,
      username,
      password
    )
  }

  /**
    * 创造出一个新的canal链接
    *
    * @return canal.MysqlConnection
    */
  def toCanalMysqlConnection: com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection = {
    val re = new com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection(address, username, password, charserNum, database)
    re.setCharset(charset)
    re.getConnector.setReceiveBufferSize(receiveBufferSize)
    re.getConnector.setSendBufferSize(sendBufferSize)
    re
  }


  /**
    * 获取一下binlog format格式
    *
    * @todo 完成它
    */
  @throws[IOException]
  private def loadBinlogFormat(mysqlConnection: MysqlConnection = this) {
    val rs: ResultSetPacket = mysqlConnection
      .query("show variables like 'binlog_format'")

  }

  /**
    * 获取一下binlog image格式
    *
    * @todo 完成它
    */
  private def loadBinlogImage(): Unit = {
    val rs: ResultSetPacket = null
  }


  def getConnector = this.connector

  @tailrec
  final def fetchUntilDefined(filterEntry: Option[CanalEntry.Entry] => Boolean)(implicit binlogParser: MysqlBinlogParser): Option[CanalEntry.Entry] = {
    lazy val event = decoder.decode(fetcher, logContext)
    lazy val entry = try {
      binlogParser.parse(Option(event))

    } catch {
      case e: CanalParseException => {
        e.getCause match {
          case IllegalArgumentException => throw new CanalParseException("fuck IllegalArgumentException when parse,id:", e.getCause)
          case _ => logger.warn(s"$e,cause:${e.getCause}"); None
        }

      }
    }
    if (filterEntry(entry)) entry else if
    (fetcher.fetch()) fetchUntilDefined(filterEntry)(binlogParser)
    else throw new Exception("impossible,unexpected end of stream")
  }

  /**
    *
    * @param binlogParser
    * @return
    */
  @tailrec
  final def fetch4Seek(implicit binlogParser: MysqlBinlogParser): CanalEntry.Entry = {
    val entry = Option(fetcher4Seek)
      .fold(throw new Exception("fetcher is null ,please seek before fetch")) {
        fetcher =>
          if (fetcher.fetch()) {
            lazy val logEvent = decoder.decode(fetcher, logContext)
            try {
              binlogParser.parse(logEvent)
            } catch {
              case e: CanalParseException => null
            }
          }
          else throw new Exception("stream unexcepted end")
      }
    if (Option(entry).isDefined) entry else fetch4Seek(binlogParser)
  }

  /**
    * seek 结束后清理环境
    */
  def cleanSeekContext: Unit = {
    fetcher4Seek = null
    logContext4Seek = null
    decoder4Seek = null
    seekFlag = false
  }
}

object MysqlConnection {
  /**
    * Canal中，preDump中做了binlogFormat/binlogImage的校验
    * 这里暂时忽略，可以再以后有必要的时候进行添加
    * 初始化metaConnection 和 tableMeta信息缓存
    */
  def preDump(mysqlConnection: MysqlConnection)(implicit binlogParser: MysqlBinlogParser): com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection = {
    //设置tableMetaCache
    val metaConnection = mysqlConnection.toCanalMysqlConnection
    val tableMetaCache: TableMetaCache = new TableMetaCache(metaConnection)
    binlogParser.setTableMetaCache(tableMetaCache)
    metaConnection
  }

  /**
    * 获取该mysql实例上所有的mysql库名
    *
    * @param mysqlConnection
    * @return List[String] 该mysql实例上所有的mysql库名
    *
    */
  def getSchemas(mysqlConnection: MysqlConnection): List[String] = {
    //如果没连接的话连接一下
    if (!mysqlConnection.isConnected) mysqlConnection.connect()
    val querySchemaCmd = "show databases"
    val fieldName = "Database"
    val ignoredDatabaseName = "information_schema"
    val list = mysqlConnection
      .query(querySchemaCmd)
      .getFieldValues
    (0 until list.size)
      .map(list.get(_))
      .filter(!_.equals(ignoredDatabaseName))
      .toList
  }

  /**
    * 从binlog拉取数据之前的设置
    */
  @throws[IOException]
  def updateSettings(mysqlConnection: MysqlConnection): Unit = {
    val settings = List(
      "set wait_timeout=9999999",
      "set net_write_timeout=1800",
      "set net_read_timeout=1800",
      "set names 'binary'",
      "set @master_binlog_checksum= @@global.binlog_checksum",
      s"set @mariadb_slave_capability='${
        LogEvent.MARIA_SLAVE_CAPABILITY_MINE
      }'"
    )
    settings.map(mysqlConnection.update(_))

  }

  /**
    * 初始化上下文，开始拉取数据
    *
    * @param binlogFileName 开始的binlog文件名
    * @param binlogPosition 开始的位点
    * @param mysqlConnection
    */
  def dump(binlogFileName: String, binlogPosition: Long)(mysqlConnection: MysqlConnection): Unit = {
    updateSettings(mysqlConnection)
    sendBinlogDump(binlogFileName, binlogPosition)(mysqlConnection)
    val connector = mysqlConnection.connector
    mysqlConnection.fetcher = new DirectLogFetcher(connector.getReceiveBufferSize)
    mysqlConnection.fetcher.start(connector.getChannel)
    mysqlConnection.decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT)
    mysqlConnection.logContext = new LogContext
    mysqlConnection.logContext.setLogPosition(new LogPosition(binlogFileName, binlogPosition))
    mysqlConnection.logContext.setFormatDescription(new FormatDescriptionLogEvent(4, mysqlConnection.binlogChecksum))
  }

  def seek(binlogFileName: String, binlogPosition: Long)(mysqlConnection: MysqlConnection): Unit = {
    updateSettings(mysqlConnection)
    sendBinlogDump(binlogFileName, binlogPosition)(mysqlConnection)
    val connector = mysqlConnection.connector
    if (!connector.isConnected) connector.connect()
    mysqlConnection.fetcher4Seek = {
      lazy val fetcher = new DirectLogFetcher()
      fetcher.start(connector.getChannel)
      fetcher
    }
    mysqlConnection.decoder4Seek = {
      lazy val decoder = new LogDecoder
      List(
        LogEvent.ROTATE_EVENT,
        LogEvent.FORMAT_DESCRIPTION_EVENT,
        LogEvent.QUERY_EVENT,
        LogEvent.XID_EVENT
      ).foreach(decoder.handle(_))
      decoder
    }
    mysqlConnection.logContext4Seek = {
      lazy val context = new LogContext
      context.setLogPosition(new LogPosition(binlogFileName))
      context
    }
    mysqlConnection.seekFlag = true
  }


  /**
    * 准备dump数据
    */
  @throws[IOException]
  private def sendBinlogDump(binlogfilename: String, binlogPosition: Long)(mysqlConnection: MysqlConnection) = {
    val binlogDumpCmd = new BinlogDumpCommandPacket
    binlogDumpCmd.binlogFileName = binlogfilename
    binlogDumpCmd.binlogPosition = binlogPosition
    binlogDumpCmd.slaveServerId = mysqlConnection.slaveId
    val cmdBody = binlogDumpCmd.toBytes
    val binlogDumpHeader = new HeaderPacket
    binlogDumpHeader.setPacketBodyLength(cmdBody.length)
    binlogDumpHeader.setPacketSequenceNumber(0x00.toByte)
    PacketManager.write(mysqlConnection.connector.getChannel, Array[ByteBuffer](ByteBuffer.wrap(binlogDumpHeader.toBytes), ByteBuffer.wrap(cmdBody)))
    mysqlConnection.connector.setDumping(true)
  }

  /**
    * 加载mysql的binlogChecksum机制
    */
  private def loadBinlogChecksum(mysqlConnection: MysqlConnection): Unit = {
    val rs: ResultSetPacket = mysqlConnection
      .query("select @master_binlog_checksum")
    val columnValues: util.List[String] = rs.getFieldValues
    mysqlConnection.binlogChecksum = if (columnValues != null && columnValues.size >= 1 && columnValues.get(0).toUpperCase == "CRC32") LogEvent.BINLOG_CHECKSUM_ALG_CRC32
    else LogEvent.BINLOG_CHECKSUM_ALG_OFF
  }

}