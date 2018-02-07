package com.neighborhood.aka.laplce.estuary.bean.resource

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

}
