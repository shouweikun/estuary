package com.neighborhood.aka.laplace.estuary.mongo

import akka.routing.ConsistentHashingRouter.ConsistentHashable
import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.mongo.source.Oplog
import org.bson.Document

/**
  * Created by john_liu on 2019/2/28.
  */
package object lifecycle {

  /**
    * oplog 使用的classifier
    *
    * @param doc 原始的mongo doc
    * @param fetchTimeStamp
    * @param partitionStrategy
    */
  final case class OplogClassifier(doc: Document, fetchTimeStamp: Long = System.currentTimeMillis(), partitionStrategy: PartitionStrategy = PartitionStrategy.PRIMARY_KEY) extends ConsistentHashable {
    override def consistentHashKey: Any = key

    lazy val ns = doc.get("ns")
    lazy val id = doc.get("h")
    lazy val key: AnyRef = partitionStrategy match {
      case PartitionStrategy.DATABASE_TABLE => ns
      case PartitionStrategy.PRIMARY_KEY => id
      case _ => id //其他的都是id
    }

    def toOplog = new Oplog(doc)
  }

}
