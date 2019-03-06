package com.neighborhood.aka.laplace.estuary.core.sink.mysql

import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.source.{MysqlHikariCpConnection, MysqlJdbcConnection}

import scala.util.Try

/**
  * Created by john_liu on 2019/1/11.
  *
  * mysql的sink
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlSinkFunc(
                           mysqlHikariCpConnection: MysqlHikariCpConnection
                         ) extends SinkFunc {
  /**
    * 生命周期
    * 关闭
    */
  override def close: Unit = mysqlHikariCpConnection.disconnect()

  /**
    * 生命周期
    * 开始
    */
  override def start: Unit = mysqlHikariCpConnection.connect()

  /**
    * 检测，是否关闭
    *
    * @return
    */
  override def isTerminated: Boolean = !mysqlHikariCpConnection.isConnected

  def insertSql(sql: String): Int = mysqlHikariCpConnection.insertSql(sql).get

  def insertBatchSql(sqls: List[String]): Try[List[Int]] = mysqlHikariCpConnection.insertBatchSql(sqls)

  def getJdbcConnection: java.sql.Connection = mysqlHikariCpConnection.getConnection

  def queryAsScalaList(sql: String): List[Map[String, AnyRef]] = {
    import MysqlJdbcConnection._
    getJdbcConnection.selectSqlAndClose(sql)
  }

}
