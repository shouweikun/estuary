package com.neighborhood.aka.laplace.estuary.core.source

import com.mysql.jdbc.Connection
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import scala.util.{Success, Try}

/**
  * Created by john_liu on 2019/1/11.
  * 使用Cp的连接池
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlHikariCpConnection(
                                     private val hikariConfig: HikariConfig
                                   ) extends DataSourceConnection {

  private lazy val ds = new HikariDataSource(hikariConfig)

  override def connect(): Unit = ds

  override def reconnect(): Unit = throw new UnsupportedOperationException("using connection pool doesn't support reconnect") //线程池不支持重连

  override def disconnect(): Unit = ds.close()

  override def isConnected: Boolean = ds.isRunning

  override def fork: MysqlHikariCpConnection = new MysqlHikariCpConnection(hikariConfig)

  /**
    * 写单条Sql
    *
    * @param sql insertSql
    * @return 插入结果
    */
  def insertSql(sql: String): Try[Int] = Try {
    val connection = ds.getConnection
    try {
      connection.setAutoCommit(true)
      connection.createStatement().executeUpdate(sql)
    } catch {
      case e =>
        throw e
    }
    finally {
      connection.close()
    }
  }


  /**
    * 插入一批Sql
    *
    * @param sqls 待插入的Sql
    * @return 插入结果
    */
  def insertBatchSql(sqls: List[String]): Try[List[Int]] = if (sqls.isEmpty) Success(List.empty) else Try {

    val conn = ds.getConnection()
    try {
      conn.setAutoCommit(false)
      val statement = conn.createStatement()
      sqls.foreach(sql => statement.addBatch(sql))
      val re = statement.executeBatch()
      conn.commit()
      statement.clearBatch()
      re.toList
    } catch {
      case e =>
        e.printStackTrace()
        throw e
    } finally {
      conn.close()
    }


  }

  def getConnection: java.sql.Connection = ds.getConnection()
}

object MysqlHikariCpConnection {
  def main(args: Array[String]): Unit = {
    val config = new HikariConfig()
    config.setJdbcUrl("jdbc:mysql://10.10.50.195")
    config.setUsername("root")
    config.setPassword("puhui123!")

    val cp = new MysqlHikariCpConnection(config)
    val a = cp.insertSql("insert into cif_monitor.test1234(name) VALUES('2')")

  }
}