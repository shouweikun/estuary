package com.neighborhood.aka.laplace.estuary.mysql.task

import java.net.InetSocketAddress
import java.nio.charset.Charset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorRef
import com.alibaba.otter.canal.common.zookeeper.ZkClientx
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection.{BinlogFormat, BinlogImage}
import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplace.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplace.estuary.bean.datasink.DataSinkBean
import com.neighborhood.aka.laplace.estuary.bean.resource.DataSourceBase
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.sink.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{RecourceManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.neighborhood.aka.laplace.estuary.mysql.utils.{LogPositionHandler, MysqlBinlogParser}
import org.apache.commons.lang.StringUtils

/**
  * Created by john_liu on 2018/2/7.
  */
class Mysql2KafkaTaskInfoManager(
                                  override val taskInfoBean: Mysql2KafkaTaskInfoBean) extends TaskManager with RecourceManager[String, MysqlConnection, KafkaSinkFunc[String]] {

  /**
    * 是否同步写，默认不是
    */
  override val isSync: Boolean = taskInfoBean.isSync
  /**
    * 是否计数，默认不计数
    */
  override val isCounting: Boolean = taskInfoBean.isCounting
  /**
    * 是否计算每条数据的时间，默认不计时
    */
  override val isCosting: Boolean = taskInfoBean.isCosting
  /**
    * 是否保留最新binlog位置
    */
  override val isProfiling: Boolean = taskInfoBean.isProfiling
  /**
    * 是否打开功率调节器
    */
  override val isPowerAdapted: Boolean = taskInfoBean.isPowerAdapted
  /**
    * 数据汇bean
    */
  override val sinkBean: DataSinkBean = taskInfoBean
  /**
    * 数据源bean
    */
  override val sourceBean: DataSourceBase = taskInfoBean
  /**
    * 监听心跳用的语句
    */
  override val delectingCommand: String = taskInfoBean.detectingSql
  /**
    * 监听重试次数标准值
    */
  override val listeningRetryTimeThreshold: Int = taskInfoBean.listenRetrytime
  /**
    * 同步任务控制器的ActorRef
    */
  val syncController: AnyRef = null
  /**
    * 传入的任务配置bean
    */
  val taskInfo = taskInfoBean
  /**
    * 同步任务标识
    */
  override val syncTaskId: String = taskInfo.syncTaskId
  /**
    * 支持的binlogFormat
    */
  lazy val supportBinlogFormats = Option(taskInfo.binlogFormat)
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
  lazy val supportBinlogImages = Option(taskInfo.binlogImages)
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
    * 同步任务开始entry
    */
  val startPosition: EntryPosition = if (StringUtils.isEmpty(this.taskInfo.journalName)) {
    if (this.taskInfo.timestamp <= 0) null else new EntryPosition(this.taskInfo.timestamp)
  } else new EntryPosition(this.taskInfo.journalName, this.taskInfo.position)
  /**
    * canal的mysqlConnection
    */
  val mysqlConnection = source
  /**
    * kafka客户端
    */
  val kafkaSink = sink
  /**
    * kafka客户端,专门处理DDL
    */
  lazy val kafkaDdlSink = kafkaSink.fork
  /**
    * batcher数量
    */
  override val batcherNum = taskInfo.batcherNum
  /**
    * sinker数量，在InOrder模式下需要使用
    */
  lazy val sinkerNum = batcherNum
  /**
    * MysqlBinlogParser
    */
  lazy val binlogParser: MysqlBinlogParser = buildParser
  /**
    * logPosition处理器
    */
  lazy val logPositionHandler: LogPositionHandler = buildEntryPositionHandler
  /**
    * 拉取数据的延迟
    */
  override lazy val fetchDelay: AtomicLong = taskInfo.fetchDelay
  /**
    * 计数器
    */
  var processingCounter: Option[ActorRef] = None
  /**
    * 功率控制器
    */
  var powerAdapter: Option[ActorRef] = None
  /**
    * 打包阈值
    */
  override lazy val batchThreshold: AtomicLong = taskInfo.batchThreshold
  /**
    * 该mysql实例上所有的mysql库名
    */
  var mysqlDatabaseNameList: List[String] = _

  /**
    * 实现@trait ResourceManager
    *
    * @return canal的mysqlConnection
    */
  override def buildSource: MysqlConnection = buildMysqlConnection()

  /**
    * 实现@trait ResourceManager
    *
    * @return KafkaSinkFunc
    */
  override def buildSink: KafkaSinkFunc[String] = {
    new KafkaSinkFunc[String](this.taskInfo)
  }

  /**
    * @return canal的mysqlConnection
    */
  def buildMysqlConnection(
                            connectionCharsetNumber: Byte = taskInfo.connectionCharsetNumber, connectionCharset: Charset = taskInfo.connectionCharset, receiveBufferSize: Int = taskInfo.receiveBufferSize, sendBufferSize: Int = taskInfo.sendBufferSize, masterCredentialInfo: MysqlCredentialBean = taskInfo.master
                          ): MysqlConnection = {
    val connectionAddress = new InetSocketAddress(masterCredentialInfo.address, masterCredentialInfo.port)
    val connectionUsername = masterCredentialInfo.username
    val connectionPassword = masterCredentialInfo.password
    val connectionDatabase = masterCredentialInfo.defaultDatabase
    new MysqlConnection(
      connectionCharset,
      connectionCharsetNumber,
      username = connectionUsername,
      password = connectionPassword,
      database = connectionDatabase,
      address = connectionAddress,
      receiveBufferSize = receiveBufferSize,
      sendBufferSize = sendBufferSize,
      slaveId = this.slaveId
    )
  }

  /**
    * @return 构建binlogParser
    */
  def buildParser: MysqlBinlogParser = {
    val convert = new MysqlBinlogParser
    val eventFilter: AviaterRegexFilter = if (!StringUtils.isEmpty(taskInfo.filterPattern)) new AviaterRegexFilter(taskInfo.filterPattern) else null
    val eventBlackFilter: AviaterRegexFilter = if (!StringUtils.isEmpty(taskInfo.filterBlackPattern)) new AviaterRegexFilter(taskInfo.filterBlackPattern) else null
    if (eventFilter != null) convert.setNameFilter(eventFilter)
    if (eventBlackFilter != null) convert.setNameBlackFilter(eventBlackFilter)


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

    val servers = taskInfo.zookeeperServers
    val timeout = taskInfo.zookeeperTimeout
    val zkLogPositionManager = new ZooKeeperLogPositionManager
    lazy val holdingZk = ZkClientx.getZkClient(servers)
    lazy val zkClient = Option(holdingZk).fold(new ZkClientx(servers, timeout))(zk => zk)
    zkLogPositionManager.setZkClientx(zkClient)

    new LogPositionHandler(zkLogPositionManager, slaveId = this.slaveId, destination = this.taskInfo.syncTaskId, address = new InetSocketAddress(taskInfo.master.address, taskInfo.master.port), master = Option(startPosition), binlogParser = binlogParser)

  }

  override def taskType: String = s"${taskInfo.dataSourceType}-${taskInfo.dataSyncType}-{${taskInfo.dataSinkType}}"
}

object Mysql2KafkaTaskInfoManager {
  lazy val zkClientx = null
  val taskStatusMap = new ConcurrentHashMap[String, Map[String, Status]]()
  val taskManagerMap = new ConcurrentHashMap[String, Mysql2KafkaTaskInfoManager]()

  /**
    * 任务管理器的构造的工厂方法
    */
  def buildManager(taskInfoBean: Mysql2KafkaTaskInfoBean): Mysql2KafkaTaskInfoManager = {
    val syncTaskId = taskInfoBean.syncTaskId
    val manager = new Mysql2KafkaTaskInfoManager(taskInfoBean)
    Mysql2KafkaTaskInfoManager.taskManagerMap.put(syncTaskId, manager)
    manager
  }


  def logCount(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager): Map[String, Long] = {
    lazy val fetchCount = mysql2KafkaTaskInfoManager.fetchCount.get()
    lazy val batchCount = mysql2KafkaTaskInfoManager.batchCount.get()
    lazy val sinkCount = mysql2KafkaTaskInfoManager.sinkCount.get()
    lazy val fetchCountPerSecond = mysql2KafkaTaskInfoManager.fetchCountPerSecond.get()
    lazy val batchCountPerSecond = mysql2KafkaTaskInfoManager.batchCountPerSecond.get()
    lazy val sinkCountPerSecond = mysql2KafkaTaskInfoManager.sinkCountPerSecond.get()
    Map("sinkCount" -> sinkCount, "batchCount" -> batchCount, "fetchCount" -> fetchCount, "fetchCountPerSecond" -> fetchCountPerSecond, "batchCountPerSecond" -> batchCountPerSecond, "sinkCountPerSecond" -> sinkCountPerSecond)
  }

  def logTimeCost(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager): Map[String, Long] = {
    lazy val fetchCost = mysql2KafkaTaskInfoManager.fetchCost.get()
    lazy val batchCost = mysql2KafkaTaskInfoManager.batchCost.get()
    lazy val sinkCost = mysql2KafkaTaskInfoManager.sinkCost.get()
    lazy val fetchCostPercentage = mysql2KafkaTaskInfoManager.fetchCostPercentage.get()
    lazy val batchCostPercentage = mysql2KafkaTaskInfoManager.batchCostPercentage.get()
    lazy val sinkCostPercentage = mysql2KafkaTaskInfoManager.sinkCostPercentage.get()
    Map("fetchCost" -> fetchCost, "batchCost" -> batchCost, "sinkCost" -> sinkCost, "fetchCostPercentage" -> fetchCostPercentage, "batchCostPercentage" -> batchCostPercentage, "sinkCostPercentage" -> sinkCostPercentage)
  }
}
