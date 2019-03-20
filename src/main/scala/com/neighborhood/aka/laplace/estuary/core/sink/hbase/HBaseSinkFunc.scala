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

  protected val flushSize = 1024 * 1024 * 1024
  protected val lowLimit = flushSize / 10 * 7
  protected val tableFlushSize = 1024 * 1024 * 20
  private val connectionStatus: AtomicBoolean = new AtomicBoolean(false)


  private def initConnection: Connection = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", hbaseSinkBean.HbaseZookeeperQuorum)
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", hbaseSinkBean.HabseZookeeperPropertyClientPort)
    conf.set("hbase.client.keyvalue.maxsize", "0")
    conf.set("hbase.hregion.memstore.flush.size", s"$flushSize")
    conf.set("hbase.regionserver.global.memstore.lowerLimit", s"$lowLimit")
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
    val re = conn.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]
    if (re.getWriteBufferSize < tableFlushSize) re.setWriteBufferSize(tableFlushSize)
    re
  }
}
