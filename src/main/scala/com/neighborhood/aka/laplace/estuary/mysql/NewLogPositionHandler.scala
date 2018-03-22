package com.neighborhood.aka.laplace.estuary.mysql

import java.io.IOException
import java.net.InetSocketAddress
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.alibaba.otter.canal.parse.exception.CanalParseException
import com.alibaba.otter.canal.parse.inbound.SinkFunction
import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.position.{EntryPosition, LogIdentity, LogPosition}
import com.neighborhood.aka.laplace.estuary.core.source.MysqlConnection
import com.taobao.tddl.dbsync.binlog.LogEvent
import org.apache.commons.lang.StringUtils

import scala.annotation.tailrec

/**
  * Created by john_liu on 2018/2/4.
  */
class NewLogPositionHandler(binlogParser: MysqlBinlogParser, manager: ZooKeeperLogPositionManager, master: Option[EntryPosition] = None, standby: Option[EntryPosition] = None, slaveId: Long = -1L, destination: String = "", address: InetSocketAddress) {
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
    * @param destination 其实就是taskid 作为zk记录的标识
    * @param journalName binlog文件的JournalName
    * @param offest      binlog的offset
    *                    记录 log 到 zk 中
    */
  def persistLogPosition(destination: String, journalName: String, offest: Long): Unit = {
    val logPosition = buildLastPosition(journalName, offest)
    manager.persistLogPosition(destination, logPosition)
  }

  /**
    * @param flag       是否dump失败过
    * @param connection mysqlConnection
    *                   获取开始的position
    */
  def findStartPosition(connection: MysqlConnection)(flag: Boolean): EntryPosition = {
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
    *                   寻找逻辑
    *                   首先先到zookeeper里寻址，以taskId作为唯一标识
    *                   否则检查是是否有传入的entryPosition
    *                   否则默认读取最后一个binlog
    *                   如果最后一个binlog的journalName为空
    *                   读取第一个binlog
    * @todo 主备切换
    */
  def findStartPositionInternal(connection: MysqlConnection)(flag: Boolean): EntryPosition = {
    val mysqlConnection = connection
    //用taskMark来作为zookeeper中的destination
    val logPosition = Option(logPositionManager.getLatestIndexBy(destination))
    val entryPosition = logPosition match {
      case Some(thePosition)if(StringUtils.isNotEmpty(thePosition.getPostion.getJournalName))=> {
        //如果定位失败
        if (flag) { // binlog定位位点失败,可能有两个原因:
          // 1. binlog位点被删除
          // 2.vip模式的mysql,发生了主备切换,判断一下serverId是否变化,针对这种模式可以发起一次基于时间戳查找合适的binlog位点
          //我们这个版本不存在主从，所以此处没有代买
          //todo 主备切换
          //todo logstash
        }
        // 其余情况
        thePosition.getPostion
        //        }
        //        else {
        //         我们没有主备切换，所以此处没有代码
        //          todo 针对切换的情况，考虑回退时间
        //        }
      }
      case _ => {
        //todo 主备切换
        //canal 在这里做了一个主备切换检查，我们先忽略,默认拿master的
        val position = master match {
          case None => findEndPosition(mysqlConnection)
          case Some(thePosition) => {
            val journalNameIsEmpty = StringUtils.isEmpty(thePosition.getJournalName)
            if (journalNameIsEmpty) {
              val timeStampIsDefined = (Option(thePosition).isDefined && thePosition.getTimestamp > 0L)
              if (timeStampIsDefined) {
                //todo log
                findByStartTimeStamp(mysqlConnection, thePosition.getTimestamp)(flag)
              } else {
                //todo log
                findEndPosition(mysqlConnection)
              }
            } else {
              if (Option(thePosition.getPosition).isDefined && thePosition.getPosition >= 0L) {
                //todo log
                thePosition
              } else {
                //todo log
                var specificLogFilePosition: EntryPosition = null
                if (thePosition.getTimestamp != null && thePosition.getTimestamp > 0L) { // 如果指定binlogName +
                  // timestamp，但没有指定对应的offest，尝试根据时间找一下offest
                  val endPosition: EntryPosition = findEndPosition(mysqlConnection)
                  if (endPosition != null) {
                    //todo log
                    specificLogFilePosition = findAsPerTimestampInSpecificLogFile(mysqlConnection, thePosition.getTimestamp, endPosition, thePosition.getJournalName)
                  }
                }
                if (specificLogFilePosition == null) {
                  // position不存在，从文件头开始
                  //4就是代表了文件头 BINLOG_START_OFFSET = 4
                  thePosition.setPosition(LogPositionHandler.BINLOG_START_OFFEST)
                  return thePosition
                }
                return specificLogFilePosition
              }
            }

          }
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
      val endPosition = new EntryPosition(fields.get.get(0), fields.get.get(1).toLong)
      endPosition
    } catch {
      case e: IOException => {
        throw new CanalParseException(" command : 'show master status' has an error!", e);
      }
    }

  }

  /**
    * 寻找事务开始的position
    * 这个方法也仅仅是做了一点scala风格的修改
    */
  def findTransactionBeginPosition(mysqlConnection: MysqlConnection, entryPosition: EntryPosition): Long = // 尝试找到一个合适的位置
  {
    val reDump = new AtomicBoolean(false)
    val address = mysqlConnection.getConnector.getAddress
    mysqlConnection.synchronized(mysqlConnection.reconnect())
    mysqlConnection.seek(entryPosition.getJournalName, entryPosition.getPosition)
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
            lastPosition = buildLastPositionByEntry(entry.get, address)
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
    * @param entry   Canal Entry
    * @param address mysql地址
    *                从entry 构建成 LogPosition
    * @todo 梳理逻辑
    */

  def buildLastPositionByEntry(entry: CanalEntry.Entry, address: InetSocketAddress = this.address) = {
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

  /**
    * @param journalName binlog文件名
    * @param offset      文件偏移量
    * @param address     mysql地址
    *                    从entry 构建成 LogPosition
    */

  def buildLastPosition(journalName: String, offset: Long, address: InetSocketAddress = this.address) = {
    val logPosition = new LogPosition
    val position = new EntryPosition
    position.setJournalName(journalName)
    position.setPosition(offset)
    logPosition.setPostion(position)
    val identity = new LogIdentity(address, -1L)
    logPosition.setIdentity(identity)
    logPosition
  }

  def findByStartTimeStamp(mysqlConnection: MysqlConnection, startTimeStamp: Long)(flag: Boolean): EntryPosition = {
    val endPosition = findEndPosition(mysqlConnection)
    val startPosition = findStartPosition(mysqlConnection)(flag)
    val maxBinlogFileName = endPosition.getJournalName
    val minBinlogFileName = startPosition.getJournalName

    @tailrec
    def loopSearch(currentSearchBinlogFile: String = maxBinlogFileName): EntryPosition = {
      val entryPosition = findAsPerTimestampInSpecificLogFile(mysqlConnection, startTimeStamp, endPosition, currentSearchBinlogFile)
      (Option(entryPosition)) match {
        case None => {
          //为true表示已经遍历到头
          StringUtils.equalsIgnoreCase(minBinlogFileName, currentSearchBinlogFile) match {
            case true => {
              //todo log
              null
            }
            case false => {
              val binlogSeqNum = currentSearchBinlogFile.substring(currentSearchBinlogFile.indexOf(".") + 1).toInt
              if (binlogSeqNum <= 1) {
                //todo logstash
                null
              } else {
                val nextBinlogSeqNum = binlogSeqNum - 1
                val binlogFileNamePrefix = currentSearchBinlogFile.substring(0, currentSearchBinlogFile.indexOf(".") + 1)
                val binlogFileNameSuffix = nextBinlogSeqNum.toString.formatted("%6d").toString
                val nextSearchBinlogFile = binlogFileNamePrefix + binlogFileNameSuffix
                loopSearch(nextSearchBinlogFile)
              }
            }
          }
        }
        case Some(theEntryPosition) => {
          theEntryPosition
        }
      }
    }

    loopSearch()
  }

  /**
    * 注：canal原生的方法，这里仅进行最小程度的scala风格修改，其余均保留原来的样式
    * 根据给定的时间戳，在指定的binlog中找到最接近于该时间戳(必须是小于时间戳)的一个事务起始位置。
    * 针对最后一个binlog会给定endPosition，避免无尽的查询
    *
    * @todo test
    */
  private[estuary] def findAsPerTimestampInSpecificLogFile(mysqlConnection: MysqlConnection, startTimestamp: Long, endPosition: EntryPosition, searchBinlogFile: String): EntryPosition = {
    val logPosition = new LogPosition
    try {
      //重启一下
      mysqlConnection.synchronized(mysqlConnection.reconnect)
      // 开始遍历文件
      mysqlConnection.seek(searchBinlogFile, 4L, new SinkFunction[LogEvent]() {
        private var lastPosition: LogPosition = null

        override def sink(event: LogEvent): Boolean = {
          var entryPosition: EntryPosition = null
          try {
            val entry = binlogParser.parse(Option(event))
            if (entry.isEmpty) return true
            val logfilename = entry.get.getHeader.getLogfileName
            val logfileoffset = entry.get.getHeader.getLogfileOffset
            val logposTimestamp = entry.get.getHeader.getExecuteTime
            if (CanalEntry.EntryType.TRANSACTIONBEGIN == entry.get.getEntryType || CanalEntry.EntryType.TRANSACTIONEND == entry.get.getEntryType) {
              //              logger.debug("compare exit condition:{},{},{}, startTimestamp={}...", Array[AnyRef](logfilename, logfileoffset, logposTimestamp, startTimestamp))
              // 事务头和尾寻找第一条记录时间戳，如果最小的一条记录都不满足条件，可直接退出
              if (logposTimestamp >= startTimestamp) return false
            }
            if (StringUtils.equals(endPosition.getJournalName, logfilename) && endPosition.getPosition <= (logfileoffset + event.getEventLen)) return false
            // 记录一下上一个事务结束的位置，即下一个事务的position
            // position = current +
            // data.length，代表该事务的下一条offest，避免多余的事务重复
            if (CanalEntry.EntryType.TRANSACTIONEND == entry.get.getEntryType) {
              entryPosition = new EntryPosition(logfilename, logfileoffset + event.getEventLen, logposTimestamp)
              //              logger.debug("set {} to be pending start position before finding another proper one...", entryPosition)
              logPosition.setPostion(entryPosition)
            }
            else if (CanalEntry.EntryType.TRANSACTIONBEGIN == entry.get.getEntryType) { // 当前事务开始位点
              entryPosition = new EntryPosition(logfilename, logfileoffset, logposTimestamp)
              //              logger.debug("set {} to be pending start position before finding another proper one...", entryPosition)
              logPosition.setPostion(entryPosition)
            }
            lastPosition = buildLastPositionByEntry(entry.get)
          } catch {
            case e: Throwable =>
              throw new Exception("exception when find binlog by timestamp", e)
          }
          false
        }
      })
    } catch {
      case e: IOException =>
      //        logger.error("ERROR ## findAsPerTimestampInSpecificLogFile has an error", e)
    }
    if (logPosition.getPostion != null) logPosition.getPostion
    else null
  }
}




