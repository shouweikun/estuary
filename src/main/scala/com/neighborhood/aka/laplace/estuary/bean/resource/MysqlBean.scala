package com.neighborhood.aka.laplace.estuary.bean.resource

import java.nio.charset.Charset

import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection.{BinlogFormat, BinlogImage}
import com.neighborhood.aka.laplace.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplace.estuary.bean.credential.MysqlCredentialBean
import org.mongodb.morphia.annotations.Embedded

/**
  * Created by john_liu on 2018/2/7.
  */
trait MysqlBean extends DataSourceBase {
  /**
    * dataSourceType置为MYSQL
    */
  //  override var dataSourceType = DataSourceType.MYSQL
  override val dataSourceType: String = SourceDataType.MYSQL.toString
  /**
    * 主服务器（服务器地址，端口，用户名，密码）
    */
   var master: MysqlCredentialBean = null
  /**
    * 从服务器
    */
   var standby: MysqlCredentialBean = null
  /**
    * 检测的Sql
    */
  var detectingSql = "show databases"
  /**
    * 过滤条件.
    */
  var filterPattern :String= null


  var filterBlackPattern :String= null
  /**
    * 默认的channel sotimeout, 对应MysqlConnector soTimeout
    */
  var defaultConnectionTimeoutInSeconds = 30

  var receiveBufferSize:Int = 16 * 1024 * 1024

  var sendBufferSize = 16 * 1024
  /**
    * mysql字符集
    */
  var connectionCharset = Charset.forName("UTF-8")
  /**
    * 字符集number
    */
  var connectionCharsetNumber = 33.toByte

  /**
    * 过滤binlog类型
    */
  var filterQueryDcl = false
  var filterQueryDml = false
  var filterQueryDdl = false
  var filterRows = false
  var filterTableError = false
  /**
    * 过滤字样
    */
  var eventFilterPattern = ""
  /**
    * 过滤黑名单字样
    */
  var eventBlackFilterPattern = ""

}
