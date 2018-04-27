package com.neighborhood.aka.laplace.estuary


import java.net.InetSocketAddress

import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher
import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplace.estuary.mysql.{BinlogPositionHandler, MysqlBinlogParser}
import com.taobao.tddl.dbsync.binlog.{LogContext, LogDecoder, LogEvent}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by john_liu on 2018/4/26.
  */
class LogPositionHandlerTest extends FlatSpec with BeforeAndAfterAll with Matchers with MockFactory {
  val binlogParserMock = mock[MysqlBinlogParser]
  val zooKeeperLogPositionManagerMock = mock[ZooKeeperLogPositionManager]
  val inetAddress = new InetSocketAddress("test", 1)
  val logPositionHandler = new BinlogPositionHandler(binlogParser = binlogParserMock, manager = zooKeeperLogPositionManagerMock, address = inetAddress)


  override def beforeAll(): Unit = {

  }



}
