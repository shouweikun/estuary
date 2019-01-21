package com.neighborhood.aka.laplace.estuary.bean.key

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

/**
  * Created by john_liu on 2018/9/29.
  */
class PartitionValueJsonKeyPartitioner extends Partitioner {
  override def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster): Int = key.asInstanceOf[BaseDataJsonKey].partitionStrategyValue % cluster.partitionCountForTopic(topic)


  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}
