package com.neighborhood.aka.laplace.estuary.core.sink.hbase

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import com.neighborhood.aka.laplace.estuary.bean.datasink.HBaseBean
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import scala.collection.JavaConverters._

/**
  * Created by john_liu on 2019/3/14.
  */
abstract class HBaseSinkFunc(val hbaseSinkBean: HBaseBean) extends SinkFunc {


  lazy val conn = initConnection

  protected val flushSize = 1024 * 1024 * 1024
  protected val lowLimit = flushSize / 10 * 7
  protected val tableFlushSize = 1024 * 1024 * 8
  private val connectionStatus: AtomicBoolean = new AtomicBoolean(false)

  private lazy val tableHolder = new ConcurrentHashMap[String, HTable]()
  private lazy val mutatorHolder = new ConcurrentHashMap[String, BufferedMutator]()

  private def initConnection: Connection = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", hbaseSinkBean.HbaseZookeeperQuorum)
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", hbaseSinkBean.HabseZookeeperPropertyClientPort)
    conf.set("hbase.client.keyvalue.maxsize", "0")
    conf.set("hbase.hregion.memstore.flush.size", s"$flushSize")
    conf.set("hbase.regionserver.global.memstore.lowerLimit", s"$lowLimit")
    conf.set("hbase.client.write.buffer", s"$tableFlushSize")
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
    if (connectionStatus.compareAndSet(true, false)) {
      getAllHoldHTable.values.foreach(_.close())
      conn.close()
    }
  }

  /**
    * 检测，是否关闭
    *
    */
  override def isTerminated: Boolean = !connectionStatus.get()

  def getAllHoldBufferMutator: Map[String, BufferedMutator] = mutatorHolder.asScala.toMap

  def getBufferMutatorAndHold = BufferedMutator =
  def getBufferMutator(tableName: String): BufferedMutator = {
    if (!connectionStatus.get()) throw new IllegalStateException("cannot get table when connection is closed")
    conn.getBufferedMutator(TableName.valueOf(tableName))
  }

  @deprecated
  def getAllHoldHTable: Map[String, HTable] = tableHolder.asScala.toMap


  @deprecated
  def getTableAndHold(tableName: String): HTable = { //没有remove操作所以安全
    if (!tableHolder.containsKey(tableName)) {
      val table = getTable(TableName.valueOf(tableName))
      tableHolder.put(tableName, getTable(TableName.valueOf(tableName)))
      table
    }
    else tableHolder.get(tableName)
  }


  @deprecated
  def getTable(tableName: TableName): HTable = {
    if (!connectionStatus.get()) throw new IllegalStateException("cannot get table when connection is closed")
    val re = conn.getTable(tableName).asInstanceOf[HTable]
    if (re.getWriteBufferSize < tableFlushSize) re.setWriteBufferSize(tableFlushSize)
    re
  }
}
