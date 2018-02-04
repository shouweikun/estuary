package com.neighborhood.aka.laplce.estuary.mysql.actors

import akka.actor.Actor
import com.alibaba.otter.canal.parse.inbound.BinlogParser
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.LogEventConvert
import com.alibaba.otter.canal.protocol.CanalEntry
import com.taobao.tddl.dbsync.binlog.LogEvent
/**
  * Created by john_liu on 2018/2/2.
  */
 class DSPBinlogParser extends LogEventConvert{

  def parse(event:Option[LogEvent]):Option[CanalEntry.Entry] = {
    event match {
      case Some(x) => Option(parse(x))
      case None    => None
    }

  }



}
object DSPBinlogParser {
  def apply: DSPBinlogParser = new DSPBinlogParser()
}

