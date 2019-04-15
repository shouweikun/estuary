package com.neighborhood.aka.laplace.estuary.bean.key

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2018/5/6.
  *
  * @todo
  */
class MultipleJsonKeyPartitioner extends Partitioner {
  val logger = LoggerFactory.getLogger(classOf[MultipleJsonKeyPartitioner])

  private def partitionByPrimaryKey(key: Any)(implicit partitions: Int): Int = {
    key.hashCode() % partitions
  }

  private def partitionByMod(mod: Long)(implicit partitions: Int): Int = (mod % partitions) toInt

  private def partitionByDbAndTable(db: String, tb: String)(implicit partitions: Int): Int = s"$db-$tb".hashCode % partitions

  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    implicit val partitions: Int = cluster.partitionCountForTopic(topic)
    key match {
      case x: BinlogKey => {
        x.getPartitionStrategy match {
          case PartitionStrategy.MOD => math.abs(partitionByMod(x.getSyncTaskSequence))
          case PartitionStrategy.PRIMARY_KEY => math.abs(partitionByPrimaryKey(x.getPrimaryKeyValue))
          case _ => ???
        }
      }
      case x: OplogKey => {
        x.getPartitionStrategy match {
          case PartitionStrategy.PRIMARY_KEY => math.abs(partitionByPrimaryKey(x.getMongoOpsUuid))
          case PartitionStrategy.DATABASE_TABLE => math.abs(partitionByDbAndTable(x.getDbName,x.getTableName))
          case _ => ???
        }
      }
    }
  }

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}
