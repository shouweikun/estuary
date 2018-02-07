package com.neighborhood.aka.laplce.estuary.bean.credential

/**
  * Created by john_liu on 2018/2/7.
  */
class MysqlCredentialBean extends DataSourceCredentialBean{
  /**
    * 服务器地址
    */
  var address:String = null
  /**
    * 服务器端口号
    */
  var port:Int = 0
  /**
    * 服务器用户名
    */
  var username:String = null
  /**
    * 服务器密码
    */
  var password:String = null
  /**
    * 服务器密码
    */
  var defaultDatabase :String = ""
}
