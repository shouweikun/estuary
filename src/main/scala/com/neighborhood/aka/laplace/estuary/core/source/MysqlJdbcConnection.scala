package com.neighborhood.aka.laplace.estuary.core.source

import java.sql._

import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.Try

/**
  * Created by john_liu on 2018/6/19.
  */
final class MysqlJdbcConnection(
                                 // URL
                                 private val insideUrl: String = "",

                                 // MYSQL用户名
                                 private val insideUsername: String = "",
                                 // MYSQL密码
                                 private val insidePassw: String = ""
                               ) extends DataSourceConnection {

  private val logger = LoggerFactory.getLogger(classOf[MysqlJdbcConnection])
  /**
    * 链接
    */
  private var conn: Option[Connection]
  = None
  /**
    * 是否处于链接状态的标志
    */
  private var flag = false

  private def createConn: Connection = {
    Class.forName("com.mysql.jdbc.Driver").newInstance
    DriverManager.getConnection(insideUrl, insideUsername, insidePassw)
  }

  override def connect(): Unit = {
    if (flag) logger.warn("thie connection has been connected") else {
      conn = Option(createConn)
    }
    flag = true
  }

  override def reconnect(): Unit = {
    conn.map(_.close())
    conn = Option(createConn)
    flag = true
  }

  override def disconnect(): Unit = {
    conn.map(_.close())
    conn = None
    flag = false
  }

  override def isConnected: Boolean = flag && conn.fold(false)(!_.isClosed)

  override def fork: MysqlJdbcConnection
  = new MysqlJdbcConnection(insideUrl, insideUsername, insidePassw)

  /**
    * ResultSet To List
    * 复用之前的方法
    */
  @throws[SQLException]
  def resultSetMetaDataToArrayList(rs: ResultSet): List[Map[String, Any]] = {

    val rsmd: ResultSetMetaData = rs.getMetaData
    val fieldList = (1 to rsmd.getColumnCount).map(index => (index, rsmd.getColumnName(index).toLowerCase.trim))

    @tailrec
    def loopBuildList(acc: List[Map[String, Any]] = List.empty): List[Map[String, Any]] = {
      if (rs.next()) {
        lazy val row = fieldList.map {
          case (index, fieldName) =>
            (fieldName -> rs.getString(index))
        }.toMap
        loopBuildList(acc.::(row))
      } else {
        rs.close()
        acc
      }
    }

    loopBuildList()

  }

  /**
    * SELECT
    */
  @throws[SQLException]
  def selectSql(sql: String): List[Map[String, Any]] = {
    logger.info(sql)
    if (!isConnected) connect()
    conn.fold(throw new SQLException("connection is not connected,is null")) {
      theConn =>
        lazy val rs = theConn.createStatement().executeQuery(sql)
        resultSetMetaDataToArrayList(rs)
    }

  }

  @throws[SQLException]
  def selectSqlAndClose(sql: String): List[Map[String, AnyRef]] = {
    val re = selectSql(sql)
    Try(this.disconnect())
    re
  }

  /**
    * INSERT DELETE UPDATE
    */
  def insertSql(sql: String): Int = {
    logger.info(sql)
    if (!isConnected) connect()
    conn.fold(throw new SQLException("connection is not connected,is null")) {
      theConn =>
        theConn.createStatement().executeUpdate(sql)
    }
  }

  /**
    * 预编译模式执行
    *
    * @param sqlPrefix
    * @param paramList
    * @return
    */
  def insertPrepareSql(sqlPrefix: String, paramList: List[(String, Any)]): Int = {
    //    if (!isConnected) connect() //因为是在一个事务中使用，所以不能尝试重连
    conn.fold(throw new SQLException("connection is not connected,is null")) {
      theConn =>
        lazy val prepareStat = theConn.prepareStatement(sqlPrefix)
        (1 to paramList.size)
          .map {
            index =>
              lazy val paramIndex = index - 1
              lazy val paramType = paramList(paramIndex)._1.toLowerCase.trim
              lazy val value = paramList(paramIndex)._2
              paramType match {
                //todo 不完全
                case "int" => prepareStat.setInt(index, value.asInstanceOf[Int])
                case "long" => prepareStat.setLong(index, value.asInstanceOf[Long])
                case "string" => prepareStat.setString(index, value.asInstanceOf[String])
                case _ => println(paramType)
              }
          }
        prepareStat
        prepareStat.executeUpdate()
    }
  }

  def startTransaction = {
    logger.info("start transaction")
    if (!isConnected) connect()
    conn.fold(throw new SQLException("connection is not connected,is null"))(_.setAutoCommit(false))
  }

  def commit = {
    logger.info("commit transaction")
    //    if (!isConnected) connect() //因为是在一个事务中使用，所以不能尝试重连
    conn.fold(throw new SQLException("connection is not connected,is null"))(_.commit())
  }

  def setConnection(connection: Connection): Unit = {
    conn = Option(connection)
  }
}

object MysqlJdbcConnection {

  implicit def MysqlJdbcConnectionHolder(connection: Connection): MysqlJdbcConnection = {
    val re = new MysqlJdbcConnection
    re.setConnection(connection)
    re
  }
}
