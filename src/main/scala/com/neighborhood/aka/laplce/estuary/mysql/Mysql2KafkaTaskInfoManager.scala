package com.neighborhood.aka.laplce.estuary.mysql

import java.net.InetSocketAddress
import java.nio.charset.Charset

import com.alibaba.otter.canal.common.zookeeper.ZkClientx
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.TableMetaCache
import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager
import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.core.task.{RecourceManager, TaskManager}
import com.typesafe.config.Config

/**
  * Created by john_liu on 2018/2/7.
  */
class Mysql2KafkaTaskInfoManager(commonConfig: Config, taskInfoBean: Mysql2KafkaTaskInfoBean) extends TaskManager with RecourceManager {

  /**
    * 同步任务控制器的ActorRef
    */
  val syncController :AnyRef = null
  /**
    * 配置文件中的配置
    */
  val config = commonConfig
  /**
    * 传入的任务配置bean
    */
  val taskInfo = taskInfoBean
  /**
    * 利用canal模拟的mysql从库的slaveId
    */
  val slaveId = taskInfoBean.slaveId
  /**
    * canal的mysqlConnection
    */
  val mysqlConnection = buildMysqlConnection
  /**
    * canal的metaConnection
    */
  val metaConnection = mysqlConnection.fork()
  /**
    * canal的TableMetaCache
    */
  val tableMetaCache: TableMetaCache = new TableMetaCache(metaConnection)
  /**
    * logPosition处理器
    */
  val logPositionFinder: LogPositionHandler = buildEntryPositionHandler
  val binlogParser : MysqlBinlogParser = new MysqlBinlogParser
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
    * @return logPosition处理器
    */
  def buildEntryPositionHandler: LogPositionHandler = {
    val servers = config.getString("common.zookeeper.servers")
    val timeout = config.getInt("common.zookeeper.timeout")
    val zkLogPositionManager = new ZooKeeperLogPositionManager
    zkLogPositionManager.setZkClientx(new ZkClientx(servers, timeout))
    new LogPositionHandler(zkLogPositionManager)

  }

  override def taskType: String =
}
