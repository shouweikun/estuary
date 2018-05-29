package com.neighborhood.aka.laplace.estuary.mysql

import akka.routing.ConsistentHashingRouter.ConsistentHashable
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.RowData

import scala.util.Try

/**
  * Created by john_liu on 2018/2/3.
  */
package object lifecycle {

  case class BinlogPositionInfo(journalName: String, offest: Long,timestamp:Long = 0)

  case class IdClassifier(entry: CanalEntry.Entry, rowData: RowData) extends ConsistentHashable {

    import scala.collection.JavaConverters._

    lazy val theKey: Any = generateKey

    override def consistentHashKey: Any = {
      theKey
    }

    def generateKey: String = {
      lazy val prefix = s"${entry.getHeader.getSchemaName}@${entry.getHeader.getTableName}@"
      val key = Try {
        if (entry.getHeader.getEventType.equals(CanalEntry.EventType.DELETE)) rowData.getBeforeColumnsList.asScala.filter(_.getIsKey).mkString("_") else rowData.getAfterColumnsList.asScala.withFilter(_.getIsKey).map(_.getValue).mkString("_")
      }.getOrElse("no_key")
      prefix + key
    }
  }

  case class DatabaseAndTableNameClassifier(entry: CanalEntry.Entry) extends ConsistentHashable {
    lazy val key = s"${entry.getHeader.getSchemaName}@${entry.getHeader.getTableName}"

    override def consistentHashKey: Any = key
  }

}
