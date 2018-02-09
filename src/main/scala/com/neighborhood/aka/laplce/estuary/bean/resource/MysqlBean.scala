package com.neighborhood.aka.laplce.estuary.bean.resource

import java.nio.charset.Charset

import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection.{BinlogFormat, BinlogImage}
import com.neighborhood.aka.laplce.estuary.bean.credential.MysqlCredentialBean
import org.mongodb.morphia.annotations.Embedded

/**
  * Created by john_liu on 2018/2/7.
  */
trait MysqlBean extends DataSourceBase {
  /**
    * dataSourceType置为MYSQL
    */
  override var dataSourceType = DataSourceType.MYSQL
  /**
    * 主服务器（服务器地址，端口，用户名，密码）
    */
  @Embedded var master: MysqlCredentialBean = null
  /**
    * 从服务器
    */
  @Embedded var standby: MysqlCredentialBean = null
  /**
    * 检测的Sql
    */
  var detectingSql = "show databases"
  /**
    * 过滤条件.
    */
  var filterPattern = null


  var filterBlackPattern = null
  /**
    * 默认的channel sotimeout, 对应MysqlConnector soTimeout
    */
  var defaultConnectionTimeoutInSeconds = 30

  var receiveBufferSize = 16 * 1024 * 1024

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
}
