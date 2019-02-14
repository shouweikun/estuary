package com.neighborhood.aka.laplace.estuary.mysql

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.routing.ConsistentHashingRouter.ConsistentHashable
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{EventType, RowData}
import com.alibaba.otter.canal.protocol.position.{EntryPosition, LogPosition}
import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.core.offset.ComparableOffset
import scala.collection.mutable
import scala.util.Try

/**
  * Created by john_liu on 2018/2/3.
  */
package object lifecycle {
  private lazy val syncSequence4EntryClassifier = new AtomicLong(1)

  final case class BinlogPositionInfo(journalName: String, offset: Long, timestamp: Long = 0) extends ComparableOffset[BinlogPositionInfo] {
    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[BinlogPositionInfo]) false else {
        lazy val theObj = obj.asInstanceOf[BinlogPositionInfo]
        lazy val journalNameEqual = theObj.journalName.trim.toLowerCase == journalName.trim.toLowerCase
        lazy val offsetEqual = theObj.offset == offset
        journalNameEqual && offsetEqual
      }
    }

    override def compare(other: BinlogPositionInfo): Boolean = {
      val otherJournalFileName: Int = other.journalName.split('.')(1).toInt
      val thisJournaleFileName: Int = this.journalName.split('.')(1).toInt
      if (otherJournalFileName > thisJournaleFileName) return true
      if (otherJournalFileName < thisJournaleFileName) return false
      if (other.offset > this.offset) true else false
    }
  }

  object BinlogPositionInfo {

    implicit class EntryPositionToBinlogPositionInfoSyntax(x: EntryPosition) {
      def toBinlogPosition: BinlogPositionInfo = {
        BinlogPositionInfo(
          x.getJournalName,
          x.getPosition,
          x.getTimestamp
        )
      }
    }

    implicit class LogPositionToBinlogPositionInfoSyntax(x: LogPosition) {
      def toBinlogPositionInfo: BinlogPositionInfo
      = BinlogPositionInfo(
        x.getPostion.getJournalName,
        x.getPostion.getPosition,
        x.getPostion.getTimestamp
      )
    }

    implicit class toLogPositionSyntax(x: BinlogPositionInfo) {
      def toLogPosition: LogPosition = {
        val re = new LogPosition
        re.setPostion(new EntryPosition(x.journalName, x.offset, x.timestamp))
        re
      }
    }

  }

  /**
    * estuary封装的RowData信息
    *
    * @param dbName             数据库名称
    * @param tableName          表名称
    * @param dmlType            DML类型
    * @param columnList
    * @param binlogPositionInfo 数据库位点信息
    * @param overrideSql        此项代表是否有指定的Sql，来覆盖计算出的结果
    */
  final case class MysqlRowDataInfo(val dbName: String,
                                    val tableName: String,
                                    val dmlType: CanalEntry.EventType,
                                    val columnList: List[CanalEntry.Column],
                                    binlogPositionInfo: BinlogPositionInfo,
                                    overrideSql: List[String] = List.empty
                                   ) {


    val sql: List[String] = overrideSql //todo


  }


  final case class EntryKeyClassifier(entry: CanalEntry.Entry, rowData: RowData, partitionStrategy: PartitionStrategy = PartitionStrategy.PRIMARY_KEY) extends ConsistentHashable {

    import scala.collection.JavaConverters._

    lazy val dbAndTb: String = s"${entry.getHeader.getSchemaName}@${entry.getHeader.getTableName}"
    lazy val thePrimaryKey: String = generatePrimaryKey
    lazy val syncSequence: Long = syncSequence4EntryClassifier.getAndIncrement()
    lazy val columnList: List[CanalEntry.Column] = {
      val list = if (entry.getHeader.getEventType.equals(CanalEntry.EventType.DELETE)) rowData.
        getBeforeColumnsList else rowData.getAfterColumnsList
      list.asScala.toList
    }


    override def consistentHashKey: Any = {
      partitionStrategy match {
        case PartitionStrategy.PRIMARY_KEY => thePrimaryKey
        case PartitionStrategy.DATABASE_TABLE => dbAndTb
        case _ => syncSequence
      }
      thePrimaryKey
    }

    def getHashKeyValue: Int = math.abs(consistentHashKey.hashCode())

    private def generatePrimaryKey: String = {
      lazy val prefix = s"$dbAndTb@"
      lazy val key = Try {
        columnList
          .withFilter(_.getIsKey)
          .map(_.getValue)
          .mkString("_")
      }.getOrElse("")
      lazy val finalKey = if (key.trim == "") UUID.randomUUID().toString else key
      prefix + finalKey
    }


  }


  final case class DatabaseAndTableNameClassifier(entry: CanalEntry.Entry) extends ConsistentHashable {
    lazy val key = s"${entry.getHeader.getSchemaName}@${entry.getHeader.getTableName}"

    override def consistentHashKey: Any = key
  }

}
