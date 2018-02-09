package com.neighborhood.aka.laplce.estuary.mysql

import java.io.IOException
import java.net.InetSocketAddress
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.alibaba.otter.canal.common.utils.JsonUtils
import com.alibaba.otter.canal.parse.exception.CanalParseException
import com.alibaba.otter.canal.parse.inbound.{ErosaConnection, SinkFunction}
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.position.{EntryPosition, LogIdentity, LogPosition}
import com.taobao.tddl.dbsync.binlog.LogEvent
import org.apache.commons.lang.StringUtils

/**
  * Created by john_liu on 2018/2/4.
  */
class LogPositionHandler(binlogParser: MysqlBinlogParser, manager: ZooKeeperLogPositionManager, master: Option[EntryPosition] = None, standby: Option[EntryPosition] = None, slaveId: Long = -1L) {
  val logPositionManager = manager

  /**
    * @param destination 其实就是taskid 作为zk记录的标识
    * @param logPosition 待被记录的log
    *                    记录 log 到 zk 中
    */
  def persistLogPosition(destination: String, logPosition: LogPosition): Unit = {
    manager.persistLogPosition(destination, logPosition)
  }

  /**
    * @param connection mysqlConnection
    * @param flag       是否失败过
    *                   获取开始的position
    */
  def findStartPosition(connection: ErosaConnection)(flag: Boolean): EntryPosition = {
    findStartPositionInternal(connection)(flag)
  }

  /**
    * @param connection mysqlConnection
    * @param flag       是否失败过
    *                   主要是应对@TableIdNotFoundException 寻找事务开始的头
    */
  def findStartPositionWithinTransaction(connection: MysqlConnection)(flag: Boolean): EntryPosition = {
    val startPosition = findStartPositionInternal(connection)(flag)
    val preTransactionStartPosition = findTransactionBeginPosition(connection, startPosition)
    if (!preTransactionStartPosition.equals(startPosition.getPosition)) {
      startPosition.setPosition(preTransactionStartPosition)
    }
    startPosition
  }

  /**
    *
    * @param connection mysqlConnection
    * @param failed     是否失败过
    *                   寻找逻辑
    *                   首先先到zookeeper里寻址，以taskId作为唯一标识
    *                   否则检查是是否有传入的entryPosition
    *                   否则默认读取最后一个binlog
    * @todo 主备切换
    * @todo timeStamp
    */
  def findStartPositionInternal(connection: ErosaConnection)(failed: Boolean): EntryPosition = {
    val mysqlConnection = connection.asInstanceOf[MysqlConnection]
    //用taskMark来作为zookeeper中的destination
    val destination = "todo"
    val logPosition = Option(logPositionManager.getLatestIndexBy(destination))

    val entryPosition = logPosition match {
      case Some(logPosition) => {
        //        if (logPosition.getIdentity.getSourceAddress == mysqlConnection.getConnector.getAddress) {
        //如果定位失败
        if (failed) { // binlog定位位点失败,可能有两个原因:
          // 1. binlog位点被删除
          // 2.vip模式的mysql,发生了主备切换,判断一下serverId是否变化,针对这种模式可以发起一次基于时间戳查找合适的binlog位点
          //todo 主备切换
        }
        // 其余情况
        logPosition.getPostion
        //        }
        //        else {
        //          todo 针对切换的情况，考虑回退时间
        //        }

      }
      case None => {
        //todo 主备切换
        //canal 在这里做了一个主备切换检查，我们先忽略,默认拿master的
        val position = master.getOrElse {
          findEndPosition(mysqlConnection)
        }

        if (StringUtils.isEmpty(position.getJournalName)) {
          //todo timeStamp模式
          position.getTimestamp
        }
        position
      }
    }
    entryPosition
  }

  /**
    * 利用`show master status`语句查找当前最新binlog
    *
    */
  def findEndPosition(mysqlConnection: MysqlConnection): EntryPosition = {
    try {
      val packet = mysqlConnection.query("show master status")
      val fields = Option(packet.getFieldValues)

      if (fields.isEmpty || fields.get.isEmpty) throw new CanalParseException("command : 'show master status' has an error! pls check. you need (at least one of) the SUPER,REPLICATION CLIENT privilege(s) for this operation")
      val endPosition = new EntryPosition(fields.get.get(0), (fields.get.get(1)).toLong)
      endPosition
    } catch {
      case e: IOException => {
        throw new CanalParseException(" command : 'show master status' has an error!", e);
      }
    }

  }

  /**
    * 寻找事务开始的position
    *
    * @todo 梳理逻辑
    */
  def findTransactionBeginPosition(mysqlConnection: MysqlConnection, entryPosition: EntryPosition): Long = // 尝试找到一个合适的位置
  {
    val reDump = new AtomicBoolean(false)
    val address = mysqlConnection.getConnector.getAddress
    mysqlConnection.synchronized(mysqlConnection.reconnect())
    mysqlConnection.seek(entryPosition.getJournalName, entryPosition.getPosition, new SinkFunction[LogEvent]() {
      private var lastPosition: LogPosition = null

      def sink(event: LogEvent): Boolean = try {
        val entry = binlogParser.parseAndProfilingIfNecessary(event, false)
        if (entry.isEmpty) return true
        // 直接查询第一条业务数据，确认是否为事务Begin/End
        if ((CanalEntry.EntryType.TRANSACTIONBEGIN eq entry.get.getEntryType) || (CanalEntry.EntryType.TRANSACTIONEND eq entry.get.getEntryType)) {
          lastPosition = buildLastPosition(entry.get, address)
          false
        }
        else {
          reDump.set(true)
          lastPosition = buildLastPosition(entry.get, address)
          false
        }
      } catch {
        case e: Exception =>
          // 上一次记录的poistion可能为一条update/insert/delete变更事件，直接进行dump的话，会缺少tableMap事件，导致tableId未进行解析
          reDump.set(true)
          false
      }
    })
    // 针对开始的第一条为非Begin记录，需要从该binlog扫描
    if (reDump.get) {
      val preTransactionStartPosition = new AtomicLong(0L)
      mysqlConnection.reconnect()
      mysqlConnection.seek(entryPosition.getJournalName, 4L, new SinkFunction[LogEvent]() {
        private var lastPosition: LogPosition = null

        override def sink(event: LogEvent): Boolean = {
          try {
            val entry = binlogParser.parseAndProfilingIfNecessary(event, false)
            if (entry.isEmpty) return true
            // 直接查询第一条业务数据，确认是否为事务Begin
            // 记录一下transaction begin position
            if ((entry.get.getEntryType eq CanalEntry.EntryType.TRANSACTIONBEGIN) && entry.get.getHeader.getLogfileOffset < entryPosition.getPosition) preTransactionStartPosition.set(entry.get.getHeader.getLogfileOffset)
            if (entry.get.getHeader.getLogfileOffset >= entryPosition.getPosition) return false // 退出
            lastPosition = buildLastPosition(entry.get, address)
            true
          } catch {
            case e: Exception =>

              false
          }

        }
      })
      // 判断一下找到的最接近position的事务头的位置
      if (preTransactionStartPosition.get > entryPosition.getPosition) {
        //        logger.error("preTransactionEndPosition greater than startPosition from zk or localconf, maybe lost data")
        throw new CanalParseException("preTransactionStartPosition greater than startPosition from zk or localconf, maybe lost data")
      }
      return preTransactionStartPosition.get
    }
    else return entryPosition.getPosition

  }

  /**
    * @param entry Canal Entry
    * @param address mysql地址
    * 从entry 构建成 LogPosition
    * @todo 梳理逻辑
    */

  def buildLastPosition(entry: CanalEntry.Entry, address: InetSocketAddress) = {
    val logPosition = new LogPosition
    val position = new EntryPosition
    position.setJournalName(entry.getHeader.getLogfileName)
    position.setPosition(entry.getHeader.getLogfileOffset)
    position.setTimestamp(entry.getHeader.getExecuteTime)
    position.setServerId(entry.getHeader.getServerId)
    logPosition.setPostion(position)
    val identity = new LogIdentity(address, -1L)
    logPosition.setIdentity(identity)
    logPosition
  }
}


