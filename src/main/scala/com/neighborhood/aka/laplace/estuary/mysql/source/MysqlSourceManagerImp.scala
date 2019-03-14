package com.neighborhood.aka.laplace.estuary.mysql.source

import java.net.InetSocketAddress

import com.alibaba.otter.canal.common.zookeeper.ZkClientx
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter
import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplace.estuary.core.task.SourceManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch.{FetchContextInitializer, FetchEntryHandler}
import com.neighborhood.aka.laplace.estuary.mysql.utils.{LogPositionHandler, MysqlBinlogParser}
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by john_liu on 2019/1/10.
  *
  * @author neighborhood.aka.laplace
  * @note 增加新资源，一定要在开始方法里面添加
  */
trait MysqlSourceManagerImp extends SourceManager[MysqlConnection] {
  protected lazy val logger = LoggerFactory.getLogger(classOf[MysqlSourceManagerImp])

  /**
    * 每次打包大小
    */
  def batchThreshold: Long

  /**
    * 是否开启Schema管理模块
    */
  def schemaComponentIsOn: Boolean

  /**
    * 同步任务Id
    */
  def syncTaskId: String

  /**
    * mysql任务信息
    */
  override def sourceBean: MysqlSourceBeanImp

  /**
    * 同步任务开始时间
    */
  def syncStartTime: Long

  /**
    * 保存offset的zk地址
    */
  def offsetSaveZkServers: String


  /**
    * 开始的任务位点
    */
  def startPosition: Option[EntryPosition]

  /**
    * 数据源数据库拥有的表名
    */
  def mysqlDatabaseNameList: List[String] = _mysqlDatabaseNameList

  lazy val _mysqlDatabaseNameList = getSourceDatabaseList

  /**
    * zk超时时间
    */
  def offsetSaveZKTimeout: Int = 10000

  /**
    * 关心的数据库名称
    */
  def concernedDatabase: List[String] = sourceBean.concernedDatabase

  /**
    * 忽略的
    */
  def ignoredDatabase: List[String] = sourceBean.ignoredDatabase

  /**
    * 监听用的sql
    */
  def detectingSql: String = sourceBean.detectingSql

  /**
    * 监听的次数
    */
  def listenRetryTime: Int = sourceBean.listenRetryTime

  /**
    * 是否需要执行ddl
    */
  def isNeedExecuteDDL: Boolean


  def binlogParser: MysqlBinlogParser = binlogParser_

  def positionHandler: LogPositionHandler = logPositionHandler_

  def fetchContextInitializer: FetchContextInitializer = fetchContextInitializer_

  /**
    * fetchEntryHandler
    */
  def fetchEntryHandler: FetchEntryHandler = fetchEntryHandler_

  private lazy val fetchEntryHandler_ = FetchEntryHandler(this)
  private lazy val logPositionHandler_ = buildEntryPositionHandler
  private lazy val binlogParser_ : MysqlBinlogParser = buildParser
  private lazy val fetchContextInitializer_ = FetchContextInitializer(this)


  /**
    * @return 构建binlogParser
    */
  def buildParser: MysqlBinlogParser = {
    val convert = new MysqlBinlogParser
    val nameFilter: Option[AviaterRegexFilter] = sourceBean.filterPattern.flatMap { pattern => if (pattern.isEmpty) None else Option(new AviaterRegexFilter(pattern)) }
    val nameBlackFilter: Option[AviaterRegexFilter] = sourceBean.filterBlackPattern.flatMap { pattern => if (pattern.isEmpty) None else Option(new AviaterRegexFilter(pattern)) }
    nameFilter.map(convert.setNameFilter(_))
    nameBlackFilter.map(convert.setNameBlackFilter(_))
    convert.setCharset(sourceBean.connectionCharset)
    convert.setFilterQueryDcl(sourceBean.filterQueryDcl)
    convert.setFilterQueryDml(sourceBean.filterQueryDml)
    convert.setFilterQueryDdl(sourceBean.filterQueryDdl)
    convert.setFilterTimestamp(math.max(syncStartTime, 0))
    convert.setFilterRows(sourceBean.filterRows)
    convert.setFilterTableError(sourceBean.filterTableError)
    convert
  }

  /**
    * 构建寻址管理器
    *
    * @return
    */
  def buildEntryPositionHandler: LogPositionHandler = {
    val zkLogPositionManager = new ZooKeeperLogPositionManager
    lazy val holdingZk = ZkClientx.getZkClient(offsetSaveZkServers)
    lazy val zkClient = Option(holdingZk).fold(new ZkClientx(offsetSaveZkServers, offsetSaveZKTimeout))(zk => zk)
    zkLogPositionManager.setZkClientx(zkClient)

    new LogPositionHandler(zkLogPositionManager, slaveId = sourceBean.slaveId, destination = this.syncTaskId, address = new InetSocketAddress(sourceBean.master.address, sourceBean.master.port), master = startPosition, binlogParser = binlogParser)

  }

  /**
    * 构建MysqlConnection
    *
    * @return source
    */
  override def buildSource: MysqlConnection = {
    val masterCredentialInfo = sourceBean.master
    val connectionAddress = new InetSocketAddress(masterCredentialInfo.address, masterCredentialInfo.port)
    val connectionUsername = masterCredentialInfo.username
    val connectionPassword = masterCredentialInfo.password
    val connectionDatabase = masterCredentialInfo.defaultDatabase.getOrElse("")
    new MysqlConnection(
      sourceBean.connectionCharset,
      sourceBean.connectionCharsetNumber,
      username = connectionUsername,
      password = connectionPassword,
      database = connectionDatabase,
      address = connectionAddress,
      receiveBufferSize = sourceBean.receiveBufferSize,
      sendBufferSize = sourceBean.sendBufferSize,
      slaveId = sourceBean.slaveId
    )
  }

  /**
    * 获取数据库名称信息
    *
    * @return 数据库列表
    */
  protected def getSourceDatabaseList: List[String] = source.fork.toJdbcConnecton.selectSqlAndClose("show databases").map(_.values.head.toString)

  /**
    * 停止所有资源
    */
  override def closeSource: Unit = Try {
    if (source.isConnected) source.disconnect()
    if (binlogParser.isStart) binlogParser.stop()
    if (positionHandler.isStart) positionHandler.close()
  }

  /**
    * 开始
    */
  override def startSource = {
    logger.info(s"start source,id:$syncTaskId")
    source
    binlogParser.start()
    positionHandler.start()
    fetchContextInitializer
  }
}

