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

    lazy val ns = Option(doc.get("ns")).getOrElse("ns")
    lazy val mod = Option(doc.get("h")).getOrElse("h")
    lazy val id = Option(doc.get("o")).map(_.asInstanceOf[Document]).map(_.get("_id")).getOrElse("_id")
    lazy val key: AnyRef = partitionStrategy match {
      case PartitionStrategy.DATABASE_TABLE => ns
      case PartitionStrategy.PRIMARY_KEY => id
      case PartitionStrategy.MOD => mod
      case PartitionStrategy.TRANSACTION => throw new UnsupportedOperationException("transcation partition strategy is not supported")
    }

    def toOplog = new Oplog(doc)
  }


}
