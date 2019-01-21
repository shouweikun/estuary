package com.neighborhood.aka.laplace.estuary.bean.credential

import scala.beans.BeanProperty

/**
  * Created by john_liu on 2018/2/7.
  */
@BeanProperty
final case class MysqlCredentialBean(/**
                                       * 服务器地址
                                       */
                                     val address: String,

                                     /**
                                       * 服务器端口号
                                       */
                                     val port: Int,

                                     /**
                                       * 服务器用户名
                                       */
                                     val username: String,

                                     /**
                                       * 服务器密码
                                       */
                                     val password: String,

                                     /**
                                       * 服务器密码
                                       */
                                     val defaultDatabase: Option[String] = None
                                    ) extends DataSourceCredentialBean {


}
