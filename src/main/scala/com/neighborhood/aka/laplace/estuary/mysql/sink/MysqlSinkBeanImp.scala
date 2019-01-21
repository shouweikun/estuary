package com.neighborhood.aka.laplace.estuary.mysql.sink

import com.neighborhood.aka.laplace.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplace.estuary.bean.datasink.MysqlSinkBean
import com.zaxxer.hikari.HikariConfig

/**
  * Created by john_liu on 2019/1/13.
  * Mysql作为数据汇的Bean实现
  *
  * @author neighborhood.aka.laplace
  */
final case class MysqlSinkBeanImp(
                                   override val credential: MysqlCredentialBean

                                 )(
                                   val isAutoCommit: Option[Boolean] = None,
                                   val connectionTimeout: Option[Long] = None,
                                   val maximumPoolSize: Option[Int] = None
                                 ) extends MysqlSinkBean {


  lazy val hikariConfig: HikariConfig = {
    val re = new HikariConfig()
    re.setJdbcUrl(s"jdbc:mysql://${credential.address}:${credential.port}/?useUnicode=true&characterEncoding=UTF-8")
    re.setUsername(credential.username)
    re.setPassword(credential.password)
    credential.defaultDatabase.flatMap(Option(_)).map(re.setSchema(_))
    isAutoCommit.flatMap(Option(_)).map(re.setAutoCommit(_))
    connectionTimeout.flatMap(Option(_)).map(re.setConnectionTimeout(_))
    maximumPoolSize.flatMap(Option(_)).map(re.setMaximumPoolSize(_))
    re
  }
}
