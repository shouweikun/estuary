package com.neighborhood.aka.laplace.estuary.mysql.utils

import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert
import com.alibaba.otter.canal.protocol.CanalEntry
import com.taobao.tddl.dbsync.binlog.LogEvent

/**
  * Created by john_liu on 2018/2/2.
  */
final class MysqlBinlogParser extends LogEventConvert {

  private var filterTimestamp: Long = 0


  def parse(eventOption: Option[LogEvent]): Option[CanalEntry.Entry] = eventOption.flatMap {
    event =>
      event.getHeader.getType match {
        case LogEvent.ROTATE_EVENT => Option(parse(event))
        case LogEvent.QUERY_EVENT => Option(parse(event))
        case LogEvent.XID_EVENT => Option(parse(event))
        case _ => if (event.getWhen *1000 >= filterTimestamp) Option(parse(event)) else None

      }
  }



  def setFilterTimestamp(ts: Long) = this.filterTimestamp = ts
}


