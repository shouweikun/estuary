package com.neighborhood.aka.laplace.estuary.core.source

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util
import java.util.List

import com.alibaba.otter.canal.parse.driver.mysql.packets.HeaderPacket
import com.alibaba.otter.canal.parse.driver.mysql.packets.client.BinlogDumpCommandPacket
import com.alibaba.otter.canal.parse.driver.mysql.packets.server.ResultSetPacket
import com.alibaba.otter.canal.parse.driver.mysql.utils.PacketManager
import com.alibaba.otter.canal.parse.driver.mysql.{MysqlConnector, MysqlQueryExecutor, MysqlUpdateExecutor}
import com.alibaba.otter.canal.parse.exception.CanalParseException
import com.alibaba.otter.canal.parse.inbound.BinlogParser
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection.{BinlogFormat, BinlogImage}
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.{DirectLogFetcher, TableMetaCache}
import com.neighborhood.aka.laplace.estuary.mysql.MysqlBinlogParser
import com.taobao.tddl.dbsync.binlog.{LogContext, LogDecoder, LogEvent}

import scala.util.Try

/**
  * Created by john_liu on 2018/3/21.
  *
  * @todo
  */

class MysqlConnection(
                       private val charset: Charset = Charset.forName("UTF-8"),
                       private val binlogFormat: BinlogFormat = null,
                       private val binlogImage: BinlogImage = null,
                       private val address: InetSocketAddress = _,
                       private val username: String,
                       private val password: String
                     ) extends DataSourceConnection {

  private lazy val connector: MysqlConnector = new MysqlConnector(address, username, password)
  private var slaveId: Long = 0L
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

  override def connect(): Unit = connector.connect()

  override def reconnect(): Unit = connector.reconnect()

  override def disconnect(): Unit = connector.disconnect()

  override def isConnected: Boolean = connector.isConnected

  @throws[IOException]
  def query(cmd: String): ResultSetPacket = {
    val exector = new MysqlQueryExecutor(connector)
    exector.query(cmd)
  }

  @throws[IOException]
  def update(cmd: String): Unit = {
    val exector = new MysqlUpdateExecutor(connector)
    exector.update(cmd)
  }

  def fork: MysqlConnection = {
    new MysqlConnection(
      charset,
      binlogFormat,
      binlogImage,
      address,
      username,
      password
    )
  }

  def toCanalMysqlConnection: com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection = {
    new com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
  }


  /**
    * 从binlog拉取数据之前的设置
    */
  @throws[IOException]
  def updateSettings(mysqlConnection: MysqlConnection = this): Unit = {
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

  /**
    * 加载mysql的binlogChecksum机制
    */
  private def loadBinlogChecksum(mysqlConnection: MysqlConnection = this): Unit = {
    val rs: ResultSetPacket = mysqlConnection
      .query("select @master_binlog_checksum")
    val columnValues: util.List[String] = rs.getFieldValues
    binlogChecksum = if (columnValues != null && columnValues.size >= 1 && columnValues.get(0).toUpperCase == "CRC32") LogEvent.BINLOG_CHECKSUM_ALG_CRC32
    else LogEvent.BINLOG_CHECKSUM_ALG_OFF
  }

  /**
    * 准备dump数据
    */
  @throws[IOException]
  private def sendBinlogDump(binlogfilename: String, binlogPosition: Long)(mysqlConnection: MysqlConnection = this) = {
    val binlogDumpCmd = new BinlogDumpCommandPacket
    binlogDumpCmd.binlogFileName = binlogfilename
    binlogDumpCmd.binlogPosition = binlogPosition
    binlogDumpCmd.slaveServerId = this.slaveId
    val cmdBody = binlogDumpCmd.toBytes
    val binlogDumpHeader = new HeaderPacket
    binlogDumpHeader.setPacketBodyLength(cmdBody.length)
    binlogDumpHeader.setPacketSequenceNumber(0x00.toByte)
    PacketManager.write(mysqlConnection.connector.getChannel, Array[ByteBuffer](ByteBuffer.wrap(binlogDumpHeader.toBytes), ByteBuffer.wrap(cmdBody)))
    mysqlConnection.connector.setDumping(true)
  }

  /**
    * Canal中，preDump中做了binlogFormat/binlogImage的校验
    * 这里暂时忽略，可以再以后有必要的时候进行添加
    *
    */
  def preDump(mysqlConnection: MysqlConnection)(binlogParser: MysqlBinlogParser): com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection = {
    //设置tableMetaCache
    val metaConnection = mysqlConnection.toCanalMysqlConnection
    val tableMetaCache: TableMetaCache = new TableMetaCache(metaConnection)
    binlogParser.setTableMetaCache(tableMetaCache)
    metaConnection
  }
}