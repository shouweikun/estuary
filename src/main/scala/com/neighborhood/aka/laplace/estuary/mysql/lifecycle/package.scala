package com.neighborhood.aka.laplace.estuary.mysql

import akka.routing.ConsistentHashingRouter.ConsistentHashable
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.RowData

/**
  * Created by john_liu on 2018/2/3.
  */
package object lifecycle {

  case class BinlogPositionInfo(journalName: String, offest: Long)

  case class IdClassifier(entry: CanalEntry.Entry, rowData: RowData) extends ConsistentHashable {

    import scala.collection.JavaConverters._

    lazy val theKey: Any = generateKey

    override def consistentHashKey: Any = {
      theKey
    }

    def generateKey: String = {
      lazy val prefix = s"${entry.getHeader.getSchemaName}@${entry.getHeader.getTableName}@"
      lazy val key = if (entry.getHeader.getEventType.equals(CanalEntry.EventType.DELETE)) rowData.getBeforeColumnsList.asScala.filter(_.getIsKey).mkString("_") else rowData.getAfterColumnsList.asScala.withFilter(_.getIsKey).map(_.getValue).mkString("_")
      prefix + key
    }
  }

  case class DatabaseAndTableNameClassifier(entry: CanalEntry.Entry) extends ConsistentHashable {
    override def consistentHashKey: Any = s"${entry.getHeader.getSchemaName}@${entry.getHeader.getTableName}"
  }

}
