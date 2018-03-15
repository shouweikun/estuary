package com.neighborhood.aka.laplace.estuary.mysql

import java.util.concurrent.atomic.AtomicLong

import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert
import com.alibaba.otter.canal.protocol.CanalEntry
import com.taobao.tddl.dbsync.binlog.LogEvent

/**
  * Created by john_liu on 2018/2/2.
  */
class MysqlBinlogParser(necessary: Boolean) extends LogEventConvert {

  lazy val count = new AtomicLong(0)

  def parse(event: Option[LogEvent]): Option[CanalEntry.Entry] = {
    event match {
      case Some(x) => Option(parse(x))
      case None => None
    }

  }
   @deprecated
  def parseAndProfilingIfNecessary(event: LogEvent, necessary: Boolean = this.necessary): Option[CanalEntry.Entry] = {

    if (necessary) {
      
    }
    parse(Option(event))
  }


}


