package com.neighborhood.aka.laplace.estuary.mysql

import java.io.IOException
import java.net.InetSocketAddress
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.alibaba.otter.canal.parse.exception.CanalParseException
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher
import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.position.{EntryPosition, LogIdentity, LogPosition}
import com.neighborhood.aka.laplace.estuary.core.source.MysqlConnection
import com.taobao.tddl.dbsync.binlog.{LogContext, LogDecoder}
import org.apache.commons.lang.StringUtils
import org.springframework.util.CollectionUtils

import scala.annotation.tailrec
import scala.util.Try

/**
  * Created by john_liu on 2018/2/4.
  */
class NewLogPositionHandler(
                             implicit binlogParser: MysqlBinlogParser,
                             manager: ZooKeeperLogPositionManager,
                             master: Option[EntryPosition] = None,
                             standby: Option[EntryPosition] = None,
                             slaveId: Long = -1L, destination: String = "",
                             address: InetSocketAddress
                           ) {
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
    * @param connection mysqlConnection
    *                   获取开始的position
    */
  def findStartPosition(connection: MysqlConnection): EntryPosition = {
    findStartPositionInternal(connection)
  }

  /**
    * @param connection mysqlConnection
    *                   主要是应对@TableIdNotFoundException 寻找事务开始的头
    */
  def findStartPositionWithinTransaction(connection: MysqlConnection): EntryPosition = {
    val startPosition = findStartPositionInternal(connection)
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
    *                   否则检查是是否有传入的entryPosition并是否有效
    *                   否则默认读取最后一个binlog
    * @todo 主备切换
    */
  def findStartPositionInternal(connection: MysqlConnection): EntryPosition = {
    //第一步试图从zookeeper中拿到binlog position
    lazy val logPositionFromZookeeper = Option(logPositionManager.getLatestIndexBy(destination))

    def findBinlogPositionIfZkisEmptyOrInvaild = {
      //zookeeper未能拿到
      //todo 主备切换
      //看看是否传入entryPosition
      master
        .fold {
          //未传入logPostion
          //读取最后位置
          findEndPosition(connection)
        } {
          //传入了logPosition的话
          thePosition =>

            val journalName = thePosition.getJournalName
            val binlogPosition = thePosition.getPosition
            val timeStamp = thePosition.getTimestamp
            //jouralName是否定义
            val journalNameIsDefined = !StringUtils.isEmpty(thePosition.getJournalName)
            //时间戳是否定义
            val timeStampIsDefined = (Option(thePosition).isDefined && thePosition.getTimestamp > 0L)
            //positionOffset是否定义
            val positionOffsetIsDefined = (Option(thePosition.getPosition).isDefined && thePosition.getPosition >= 0L)
            (journalNameIsDefined, positionOffsetIsDefined, timeStampIsDefined) match {
              case (true, true, true) => thePosition
              case (true, true, false) => thePosition
              case (true, false, true) => findByStartTimeStamp(connection, timeStamp)
              case (true, false, false) => findAsPerTimestampInSpecificLogFile(connection, timeStamp, findEndPosition(connection), journalName)
              case (false, true, true) => findByStartTimeStamp(connection, timeStamp)
              case (false, true, false) => findEndPosition(connection)
              case (false, false, true) => findByStartTimeStamp(connection, timeStamp)
              case (false, false, false) => findEndPosition(connection)
            }
        }
    }

    logPositionFromZookeeper
      .fold {
        findBinlogPositionIfZkisEmptyOrInvaild
      } {
        //如果传了
        theLogPosition =>
          //binlog 被移除的话
          if (binlogIsRemoved(connection, theLogPosition.getPostion.getJournalName)) findBinlogPositionIfZkisEmptyOrInvaild else theLogPosition.getPostion
      }

  }

  /**
    * 查看binlog在mysql中是否被移除
    * 利用`show binlog event in '特定binlog'`
    * * @param mysqlConnection
    *
    * @param journalName
    * @return
    */
  private def binlogIsRemoved(mysqlConnection: MysqlConnection, journalName: String): Boolean = {
    Try {
      lazy val fields = mysqlConnection.query(s"show binlog events in '$journalName'").getFieldValues
      CollectionUtils.isEmpty(fields)
    }.getOrElse(false)
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
    * 这个方法也做了scala风格的修改
    * //todo
    */
  def findTransactionBeginPosition(mysqlConnection: MysqlConnection, entryPosition: EntryPosition): Long = // 尝试找到一个合适的位置
  {
    def prepareConnection(position: Long) = {
      mysqlConnection.reconnect()
      mysqlConnection.seek(entryPosition.getJournalName, position)(mysqlConnection)
    }

    prepareConnection(entryPosition.getPosition)
    if (mysqlConnection.fetch4Seek.getEntryType == CanalEntry.EntryType.TRANSACTIONBEGIN || mysqlConnection.fetch4Seek.getEntryType == CanalEntry.EntryType.TRANSACTIONEND) entryPosition.getPosition
    else {
      prepareConnection(4L)
      @tailrec
      lazy val loopFind: Long = {
        val theEntry = mysqlConnection.fetch4Seek
        if (theEntry.getHeader.getLogfileOffset > entryPosition.getPosition){
          throw new CanalParseException("the current entry is bigger than last when find Transaction Begin Position")
        }
          if (theEntry.getEntryType == CanalEntry.EntryType.TRANSACTIONBEGIN)
            theEntry.getHeader.getLogfileOffset else loopFind
      }
      loopFind
    }

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

  /**
    * 基于时间戳查找
    *
    * @param mysqlConnection
    * @param startTimeStamp
    * @return
    */
  def findByStartTimeStamp(mysqlConnection: MysqlConnection, startTimeStamp: Long): EntryPosition = {
    val endPosition = findEndPosition(mysqlConnection)
    val startPosition = findfirstPosition(mysqlConnection)
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
    * 查询当前binlog位置，这个主要是用于寻址
    *
    * @param mysqlConnection
    * @return
    */
  private[estuary] def findfirstPosition(mysqlConnection: MysqlConnection): EntryPosition = {
    lazy val endPosition = try {
      lazy val fields = mysqlConnection.
        query("show binlog events limit 1")
        .getFieldValues
      new EntryPosition(fields.get(0), fields.get(1).toLong)
    } catch {
      case e: Exception => throw new CanalParseException("command : 'show master status' has an error!", e)
    }
    endPosition
  }

  /**
    * 注：canal原生的方法，这里进行了scala风格修改
    * 根据给定的时间戳，在指定的binlog中找到最接近于该时间戳(必须是小于时间戳)的一个事务起始位置。
    * 针对最后一个binlog会给定endPosition，避免无尽的查询
    *
    * @todo test
    */
  private[estuary] def findAsPerTimestampInSpecificLogFile(mysqlConnection: MysqlConnection, startTimestamp: Long, endPosition: EntryPosition, searchBinlogFile: String): EntryPosition = {
    lazy val position = Try {
      //重启一下
      mysqlConnection.synchronized(mysqlConnection.reconnect)
      // 开始遍历文件
      mysqlConnection.seek(searchBinlogFile, 4L)(mysqlConnection)
      val fetcher: DirectLogFetcher = mysqlConnection.fetcher4Seek
      val decoder: LogDecoder = mysqlConnection.decoder4Seek
      val logContext: LogContext = mysqlConnection.logContext4Seek

      loopFetchAndFindEntry(fetcher, decoder, logContext)(startTimestamp, endPosition)
    }
      .getOrElse(null)
    mysqlConnection.cleanSeekContext
    position
  }

  /**
    * 在寻找binlog位置时用的方法
    *
    * @param fetcher 拉取binlog文件的DirectFetcher
    * @param decoder
    * @todo test
    */
  @tailrec
  private[estuary] def loopFetchAndFindEntry(fetcher: DirectLogFetcher, decoder: LogDecoder, logContext: LogContext)(startTimestamp: Long = 0L, endPosition: EntryPosition = null): EntryPosition = {
    if (fetcher.fetch()) {
      val event = decoder.decode(fetcher, logContext)
      val entry = try {
        binlogParser.parse(Option(event))
      } catch {
        case e: CanalParseException => {
          // log.warning(s"table has been removed")
          None
        }
      }

      /**
        * 寻找到Entry并且判断这个Entry进行处理
        * 如果比最后的时间戳还晚 -> 返回1 -> null
        * 如果比最早的时间戳还早 -> 返回1 -> null
        * 如果是事务头或者事务尾 -> 返回2 -> 以这个entry构建
        * 如果不属于上述几种情况 -> 返回3 -> 继续loopFetch
        *
        * @todo test
        */
      def findAndJudgeEntry(entry: CanalEntry.Entry): Int = {
        val logfilename = entry.getHeader.getLogfileName
        val logfileoffset = entry.getHeader.getLogfileOffset
        val logposTimestamp = entry.getHeader.getExecuteTime
        val entryType = entry.getEntryType

        //比最晚的都晚
        def lateThanLatest: Boolean = if (endPosition != null) (StringUtils.equals(endPosition.getJournalName, logfilename) && endPosition.getPosition <= (logfileoffset + event.getEventLen)) else true

        //比最早的都早
        def earlierThanEarliest: Boolean = if (startTimestamp != 0) logposTimestamp >= startTimestamp else true

        //综合两者
        def outOfTimeRequirement = (lateThanLatest && earlierThanEarliest)

        //进行判断
        if (lateThanLatest) 1 else entryType match {
          case CanalEntry.EntryType.TRANSACTIONEND => 2 //todo log
          case CanalEntry.EntryType.TRANSACTIONBEGIN => 2 //todo log
          case _ => 3 //todo log
        }
      }

      if (entry.isEmpty) loopFetchAndFindEntry(fetcher, decoder, logContext)(startTimestamp, endPosition) else {
        findAndJudgeEntry(entry.get) match {
          case 1 => null
          case 2 => new EntryPosition(entry.get.getHeader.getLogfileName, entry.get.getHeader.getLogfileOffset, entry.get.getHeader.getExecuteTime)
          case _ => loopFetchAndFindEntry(fetcher, decoder, logContext)(startTimestamp, endPosition)
        }
      }
    } else null //todo log
  }

}




