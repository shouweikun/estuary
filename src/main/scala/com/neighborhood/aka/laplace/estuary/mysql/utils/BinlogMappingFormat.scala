package com.neighborhood.aka.laplace.estuary.mysql.utils

import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat

/**
  * Created by john_liu on 2018/5/1.
  */
trait BinlogMappingFormat extends MappingFormat[CanalEntry.Entry, Array[KafkaMessage]] {



  override def transform(entry: CanalEntry.Entry): Array[KafkaMessage] = {

    val entryType = entry.getEntryType
    val header = entry.getHeader
    val eventType = header.getEventType
    val tempJsonKey = BinlogKey.buildBinlogKey(header)
    ???
  }

  /**
    * @param tempJsonKey   BinlogJsonKey
    * @param entry         entry
    * @param logfileName   binlog文件名
    * @param logfileOffset binlog文件偏移量
    * @param before        开始时间
    *                      将DDL类型的CanalEntry 转换成Json
    */
  def transferDDltoJson(tempJsonKey: BinlogKey, entry: CanalEntry.Entry, logfileName: String, logfileOffset: Long, before: Long): KafkaMessage = {
    ???
    //让程序知道是DDL
    tempJsonKey.setDbName("DDL")
    //log.info(s"batch ddl ${CanalEntryJsonHelper.entryToJson(entry)}")
    val re = new KafkaMessage(tempJsonKey, CanalEntryJsonHelper.entryToJson(entry))
    val theAfter = System.currentTimeMillis()
    tempJsonKey.setMsgSyncEndTime(theAfter)
    tempJsonKey.setMsgSyncUsedTime(theAfter - before)
    re
  }
}
