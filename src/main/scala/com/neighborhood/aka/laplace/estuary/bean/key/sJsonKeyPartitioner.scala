package com.neighborhood.aka.laplace.estuary.bean.key

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

/**
  * Created by john_liu on 2018/5/6.
  * @todo
  */
class sJsonKeyPartitioner extends Partitioner {

  private def partitionByPrimaryKey(key: Any)(implicit partitions: Int): Int = key.hashCode() % partitions

  private def partitionByMod(mod: Long)(implicit partitions: Int): Int = (mod % partitions)toInt

  private def partitionByDbAndTable(db: String, tb: String)(implicit partitions: Int): Int = s"$db-$tb".hashCode % partitions

  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    implicit val partitions = cluster.partitionCountForTopic(topic)
    key match {
      case x: BinlogKey => {
        x.getPartitionStrategy match {
          case PartitionStrategy.MOD => partitionByMod(x.getSyncTaskSequence)
          case _ => ???
        }
      }
      case x: OplogKey => {
        x.getPartitionStrategy match {
          case PartitionStrategy.PRIMARY_KEY => partitionByPrimaryKey(x.getMongoOpsUuid)
          case _ => ???
        }
      }
    }
  }

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}
