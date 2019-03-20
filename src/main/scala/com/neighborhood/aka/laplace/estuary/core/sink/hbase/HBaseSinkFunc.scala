package com.neighborhood.aka.laplace.estuary.core.sink.hbase

import java.util.concurrent.atomic.AtomicBoolean

import com.neighborhood.aka.laplace.estuary.bean.datasink.HBaseBean
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * Created by john_liu on 2019/3/14.
  */
abstract class HBaseSinkFunc(val hbaseSinkBean: HBaseBean) extends SinkFunc {


  lazy val conn = initConnection

  private val connectionStatus: AtomicBoolean = new AtomicBoolean(false)


  private def initConnection: Connection = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", hbaseSinkBean.HbaseZookeeperQuorum)
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", hbaseSinkBean.HabseZookeeperPropertyClientPort)
    conf.set("hbase.client.keyvalue.maxsize", "0")

    val conn = ConnectionFactory.createConnection(conf)

    conn
  }

  def start(): Unit = {
    conn
    assert(conn != null)
    assert(!conn.isClosed)
    connectionStatus.set(true)
  }

  override def close: Unit = {
    if (connectionStatus.compareAndSet(true, false)) conn.close()
  }

  /**
    * 检测，是否关闭
    *
    */
  override def isTerminated: Boolean = !connectionStatus.get()

  def getTable(tableName: String): HTable = {
    if (!connectionStatus.get()) throw new IllegalStateException()
    conn.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]
  }
}
