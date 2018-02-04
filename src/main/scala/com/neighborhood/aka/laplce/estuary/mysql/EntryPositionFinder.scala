package com.neighborhood.aka.laplce.estuary.mysql

import java.io.IOException

import com.alibaba.otter.canal.parse.exception.CanalParseException
import com.alibaba.otter.canal.parse.inbound.ErosaConnection
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlConnection
import com.alibaba.otter.canal.parse.index.ZooKeeperLogPositionManager
import com.alibaba.otter.canal.protocol.position.EntryPosition

/**
  * Created by john_liu on 2018/2/4.
  */
class EntryPositionFinder(manager:ZooKeeperLogPositionManager,master:Option[EntryPosition] = None,slave:Option[EntryPosition]) {
  val logPositionManager = manager
  /**
    * 寻找逻辑
    * 首先先到zookeeper里寻址，以taskMark作为唯一标识
    * 否则检查是是否有传入的entryPosition
    * 否则默认读取最后一个binlog
    * @todo 主备切换
    */
   def findStartPosition(connection: ErosaConnection): EntryPosition = {
    //用taskMark来作为zookeeper中的destination
    val destination = "todo"
    val logPosition = Option(logPositionManager.getLatestIndexBy(destination))

    val entryPosition = logPosition match {
      case Some(x) => {
        x.getPostion
      }
      case None => {
        //canal 在这里做了一个主备切换检查，我们先忽略,默认拿master的
        val position = master.getOrElse()
        position
      }
    }

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
