package com.neighborhood.aka.laplce.estuary.bean.credential

/**
  * Created by john_liu on 2018/2/7.
  */
class MysqlCredentialBean(Address:String,Port:Int,Username:String,Password:String,DefaultDatabase :String) extends DataSourceCredentialBean{
  /**
    * 服务器地址
    */
  var address:String = Address
  /**
    * 服务器端口号
    */
  var port:Int = Port
  /**
    * 服务器用户名
    */
  var username:String = Username
  /**
    * 服务器密码
    */
  var password:String = Password
  /**
    * 服务器密码
    */
  var defaultDatabase :String = DefaultDatabase


}
