package com.neighborhood.aka.laplace.estuary.bean.key
import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
/**
  * Created by john_liu on 2018/5/6.
  */
class sJsonKeyPartitioner extends Partitioner{
  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    key match {
      case x:BinlogKey =>
      case
    }
  }

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}
