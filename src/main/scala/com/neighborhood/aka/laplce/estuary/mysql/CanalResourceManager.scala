package com.neighborhood.aka.laplce.estuary.mysql

import java.net.InetSocketAddress
import java.nio.charset.Charset

import com.alibaba.otter.canal.common.zookeeper.ZkClientx
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache
import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager
import com.neighborhood.aka.laplace.estuary.bean.MysqlSyncTaskBean
import com.neighborhood.aka.laplce.estuary.mysql.actors.DSPBinlogParser
import com.typesafe.config.Config

/**
  * Created by john_liu on 2018/2/2.
  */
class MysqlTaskInfoResourceManager(commonConfig: Config, taskInfoBean: MysqlSyncTaskBean) {
  //基本配置信息
  val config = commonConfig
  //任务信息
  val taskInfo = taskInfoBean
  //slaveId
  val slaveId = System.currentTimeMillis()
  val mysqlConnection: MysqlConnection = buildMysqlConnection
  val metaConnection: MysqlConnection = mysqlConnection.fork()
  val binlogParser: DSPBinlogParser = DSPBinlogParser.apply
  val tableMetaCache: TableMetaCache = new TableMetaCache(metaConnection)
  val logPostionManager: ZooKeeperLogPositionManager = buildZooKeeperLogPositionManager
  val logPositionFinder:EntryPositionFinder = new EntryPositionFinder(logPostionManager)
  def buildZooKeeperLogPositionManager: ZooKeeperLogPositionManager = {
    val servers = config.getString("common.zookeeper.servers")
    val timeout = config.getInt("common.zookeeper.timeout")
    val zkLogPositionManager = new ZooKeeperLogPositionManager
    zkLogPositionManager.setZkClientx(new ZkClientx(servers, timeout))
    zkLogPositionManager
  }
  def buildMysqlConnection: MysqlConnection = {
    //charsetNumber
    val connectionCharsetNumber: Byte = taskInfo.getConnectionCharsetNumber
    //字符集
    val connectionCharset: Charset = taskInfo.getConnectionCharset
    val receiveBufferSize = taskInfo.getReceiveBufferSize
    val sendBufferSize = taskInfo.getSendBufferSize
    val masterCredentialInfo = taskInfo.getMaster
    val address = new InetSocketAddress(masterCredentialInfo.address, masterCredentialInfo.port)
    val username = masterCredentialInfo.username
    val password = masterCredentialInfo.password
    val database = masterCredentialInfo.database
    val conn = new MysqlConnection(address, username, password, connectionCharsetNumber, database)
    conn.setCharset(connectionCharset)
    conn.setSlaveId(slaveId)
    conn.getConnector.setSendBufferSize(sendBufferSize)
    conn.getConnector.setReceiveBufferSize(receiveBufferSize)
    conn
  }
}
object MysqlTaskInfoResourceManager {

  val defaultConfigPath = ""


  def apply(config: Config, taskInfoBean: MysqlSyncTaskBean): MysqlTaskInfoResourceManager = new MysqlTaskInfoResourceManager(config, taskInfoBean)

  //  def apply(taskInfoJson: String): MysqlTaskInfoResourceManager = {
  //    new MysqlTaskInfoResourceManager(commonConfig, taskInfoJson)
  //  }
}
