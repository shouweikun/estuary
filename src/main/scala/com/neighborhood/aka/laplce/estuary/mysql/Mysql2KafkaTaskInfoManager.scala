package com.neighborhood.aka.laplce.estuary.mysql

import java.net.InetSocketAddress
import java.nio.charset.Charset

import com.alibaba.otter.canal.common.zookeeper.ZkClientx
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection.{BinlogFormat, BinlogImage}
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache
import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager
import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.core.lifecycle.Status
import com.neighborhood.aka.laplce.estuary.core.lifecycle.Status.Status
import com.neighborhood.aka.laplce.estuary.core.sink.KafkaSinkFunc
import com.neighborhood.aka.laplce.estuary.core.task.{RecourceManager, TaskManager}
import com.typesafe.config.Config

/**
  * Created by john_liu on 2018/2/7.
  */
class Mysql2KafkaTaskInfoManager(commonConfig: Config, taskInfoBean: Mysql2KafkaTaskInfoBean) extends TaskManager with RecourceManager[String,MysqlConnection, KafkaSinkFunc[String,String]] {

  /**
    * 同步任务控制器的ActorRef
    */
  val syncController: AnyRef = null
  /**
    * 配置文件中的配置
    */
  val config = commonConfig
  /**
    * 传入的任务配置bean
    */
  val taskInfo = taskInfoBean
  /**
    * 支持的binlogFormat
    */
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
  /**
    * 支持的binlogImage
    */
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
  /**
    * 利用canal模拟的mysql从库的slaveId
    */
  val slaveId = taskInfoBean.slaveId
  /**
    * canal的mysqlConnection
    */
  val mysqlConnection = buildSource
  /**
    * kafka客户端
    */
  val kafkaSink = buildSink
  /**
    * MysqlBinlogParser
    */
  lazy val binlogParser :MysqlBinlogParser = buildParser
  /**
    * logPosition处理器
    */
  lazy val logPositionHandler: LogPositionHandler = buildEntryPositionHandler

  /**
    * fetcher的状态
    */
  @volatile
  var fetcherStatus: Status = Status.OFFLINE
  /**
    * batcher的状态
    */
  @volatile
  var batcherStatus: Status = Status.OFFLINE
  /**
    * heartbeatListener的状态
    */
  @volatile
  var heartBeatListenerStatus: Status = Status.OFFLINE
  /**
    * sinker的状态
    */
  @volatile
  var sinkerStatus: Status = Status.OFFLINE
  /**
    * syncControllerStatus的状态
    */
  @volatile
  var syncControllerStatus: Status = Status.OFFLINE

  /**
    * 实现@trait ResourceManager
    *
    * @return canal的mysqlConnection
    */
  override def buildSource: MysqlConnection = buildMysqlConnection

  /**
    * 实现@trait ResourceManager
    *
    * @return KafkaSinkFunc
    */
  override def buildSink: KafkaSinkFunc[String,String] = {
    new KafkaSinkFunc[String,String](this.taskInfo)
  }

  /**
    * @return canal的mysqlConnection
    */
  def buildMysqlConnection: MysqlConnection = {
    //charsetNumber
    val connectionCharsetNumber: Byte = taskInfo.connectionCharsetNumber
    //字符集
    val connectionCharset: Charset = taskInfo.connectionCharset
    val receiveBufferSize = taskInfo.receiveBufferSize
    val sendBufferSize = taskInfo.sendBufferSize
    val masterCredentialInfo = taskInfo.master
    val address = new InetSocketAddress(masterCredentialInfo.address, masterCredentialInfo.port)
    val username = masterCredentialInfo.username
    val password = masterCredentialInfo.password
    val database = masterCredentialInfo.defaultDatabase
    val conn = new MysqlConnection(address, username, password, connectionCharsetNumber, database)
    conn.setCharset(connectionCharset)
    conn.setSlaveId(slaveId)
    conn.getConnector.setSendBufferSize(sendBufferSize)
    conn.getConnector.setReceiveBufferSize(receiveBufferSize)
    conn
  }
  /**
    * @return 构建binlogParser
    */
  def buildParser: MysqlBinlogParser = {
    val convert = new MysqlBinlogParser(taskInfo.isProfiling)
    val eventFilter = new AviaterRegexFilter(taskInfo.filterPattern)
    val eventBlackFilter = new AviaterRegexFilter(taskInfo.filterBlackPattern)
    if (eventFilter != null && eventFilter.isInstanceOf[AviaterRegexFilter]) convert.setNameFilter(eventFilter.asInstanceOf[AviaterRegexFilter])
    if (eventBlackFilter != null && eventBlackFilter.isInstanceOf[AviaterRegexFilter]) convert.setNameBlackFilter(eventBlackFilter.asInstanceOf[AviaterRegexFilter])

    convert.setCharset(taskInfo.connectionCharset)
    convert.setFilterQueryDcl(taskInfo.filterQueryDcl)
    convert.setFilterQueryDml(taskInfo.filterQueryDml)
    convert.setFilterQueryDdl(taskInfo.filterQueryDdl)
    convert.setFilterRows(taskInfo.filterRows)
    convert.setFilterTableError(taskInfo.filterTableError)

    convert
  }

  /**
    * @return logPosition处理器
    */
  def buildEntryPositionHandler: LogPositionHandler = {
    val servers = config.getString("common.zookeeper.servers")
    val timeout = config.getInt("common.zookeeper.timeout")
    val zkLogPositionManager = new ZooKeeperLogPositionManager
    zkLogPositionManager.setZkClientx(new ZkClientx(servers, timeout))
    new LogPositionHandler(binlogParser,zkLogPositionManager,slaveId = this.slaveId,destination = this.taskInfo.syncTaskId,address = new InetSocketAddress(taskInfo.master.address,taskInfo.master.port))

  }

  override def taskType: String = s"${taskInfo.dataSourceType}-${taskInfo.dataSyncType}-{${taskInfo.dataSinkType}}"
}
