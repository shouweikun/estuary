package com.neighborhood.aka.laplce.estuary.mysql

import java.io.IOException

import com.alibaba.otter.canal.common.utils.JsonUtils
import com.alibaba.otter.canal.parse.exception.CanalParseException
import com.alibaba.otter.canal.parse.inbound.ErosaConnection
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager
import com.alibaba.otter.canal.protocol.position.{EntryPosition, LogPosition}
import org.apache.commons.lang.StringUtils

/**
  * Created by john_liu on 2018/2/4.
  */
class LogPositionHandler(manager: ZooKeeperLogPositionManager, master: Option[EntryPosition] = None, standby: Option[EntryPosition] = None) {
  val logPositionManager = manager

  /**
    *@param destination 其实就是taskid 作为zk记录的标识
    *@param logPosition 待被记录的log
    * 记录 log 到 zk 中
    */
  def persistLogPosition(destination: String, logPosition: LogPosition) :Unit= {
    manager.persistLogPosition(destination,logPosition)
  }

  def findStartPosition(connection: ErosaConnection)(flag: Boolean): EntryPosition = {
    findStartPositionInternal(connection)(flag: Boolean)
  }

  /**
    * 寻找逻辑
    * 首先先到zookeeper里寻址，以taskMark作为唯一标识
    * 否则检查是是否有传入的entryPosition
    * 否则默认读取最后一个binlog
    *
    * @todo 主备切换
    * @todo timeStamp
    */
  def findStartPositionInternal(connection: ErosaConnection)(flag: Boolean): EntryPosition = {
    val mysqlConnection = connection.asInstanceOf[MysqlConnection]
    //用taskMark来作为zookeeper中的destination
    val destination = "todo"
    val logPosition = Option(logPositionManager.getLatestIndexBy(destination))

    val entryPosition = logPosition match {
      case Some(logPosition) => {
        //        if (logPosition.getIdentity.getSourceAddress == mysqlConnection.getConnector.getAddress) {
        //如果定位失败
        if (flag) { // binlog定位位点失败,可能有两个原因:
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
}
