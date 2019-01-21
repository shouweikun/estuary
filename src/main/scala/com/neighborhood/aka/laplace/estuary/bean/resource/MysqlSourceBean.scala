package com.neighborhood.aka.laplace.estuary.bean.resource

import java.nio.charset.Charset

import com.neighborhood.aka.laplace.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection

/**
  * Created by john_liu on 2018/2/7.
  */
trait MysqlSourceBean extends DataSourceBase[MysqlConnection] {
  /**
    * dataSourceType置为MYSQL
    */
  //  override var dataSourceType = DataSourceType.MYSQL
  override val dataSourceType: String = SourceDataType.MYSQL.toString

  def slaveId: Long = System.currentTimeMillis()

  /**
    * 主服务器（服务器地址，端口，用户名，密码）
    */
  def master: MysqlCredentialBean

  /**
    * 从服务器
    */
  def standby: Option[MysqlCredentialBean] = None

  /**
    * 检测的Sql
    */
  def detectingSql: String = "show databases"

  /**
    * 过滤条件.
    */
  def filterPattern: Option[String] = None

  /**
    * 过滤条件，黑名单
    *
    * @return
    */
  def filterBlackPattern: Option[String] = None

  /**
    * 发送心跳关心的数据库
    *
    * @return
    */
  def concernedDatabase: List[String]

  /**
    * 发送心跳不关心的数据库
    *
    * @return
    */
  def ignoredDatabase: List[String]

  /**
    * 监听重试次数
    *
    * @return
    */
  def listenRetryTime: Int = 3

  /**
    * 默认的channel sotimeout, 对应MysqlConnector soTimeout
    */
  def defaultConnectionTimeoutInSeconds = 30

  def receiveBufferSize: Int = 16 * 1024 * 1024

  def sendBufferSize = 16 * 1024

  /**
    * mysql字符集
    */
  def connectionCharset = Charset.forName("UTF-8")

  /**
    * 字符集number
    */
  def connectionCharsetNumber = 33.toByte

  /**
    * 过滤binlog类型
    */
   def filterQueryDcl: Boolean = false

   def filterQueryDml: Boolean = false

   def filterQueryDdl: Boolean = false

   def filterRows: Boolean = false

   def filterTableError: Boolean = false


}
