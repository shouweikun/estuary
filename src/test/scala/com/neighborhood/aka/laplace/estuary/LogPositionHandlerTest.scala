package com.neighborhood.aka.laplace.estuary


import java.net.InetSocketAddress

import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager
import com.neighborhood.aka.laplace.estuary.mysql.{BinlogPositionHandler, MysqlBinlogParser}
import com.taobao.tddl.dbsync.binlog.{DirectLogFetcher, LogContext, LogDecoder}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by john_liu on 2018/4/26.
  */
class LogPositionHandlerTest extends FlatSpec with BeforeAndAfterAll with Matchers with MockFactory {
  val binlogParserMock = mock[MysqlBinlogParser]
  val zooKeeperLogPositionManagerMock = mock[ZooKeeperLogPositionManager]
  val inetAddress = new InetSocketAddress("test", 1)
  val fetcherMock = mock[DirectLogFetcher]
  val decoderMock = mock[LogDecoder]
  val logContextMock = mock[LogContext]
  val logPositionHandler = new BinlogPositionHandler(binlogParser = binlogParserMock, manager = zooKeeperLogPositionManagerMock, address = inetAddress)

  "loopFetchAndFindEntry" should "find Null when earlier than earliest"  in {
     logPositionHandler.loopFetchAndFindEntry(fetcherMock,decoderMock,logContextMock)
  }

}
