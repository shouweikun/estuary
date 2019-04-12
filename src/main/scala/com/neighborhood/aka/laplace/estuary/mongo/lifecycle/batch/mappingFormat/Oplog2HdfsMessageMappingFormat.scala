package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.mappingFormat

import com.neighborhood.aka.laplace.estuary.bean.support.HdfsMessage
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoOffset}
import com.neighborhood.aka.laplace.estuary.mongo.util.MongoDocumentToJson

/**
  * Created by john_liu on 2019/4/12.
  *
  * @author neighborhood.aka.laplace
  *         将oplog转换成HdfsMessage的核心类
  */
final class Oplog2HdfsMessageMappingFormat(
                                            override val mongoConnection: MongoConnection, //mongo链接
                                            override val syncTaskId: String,
                                            override val mongoDocumentToJson: MongoDocumentToJson
                                          ) extends OplogMappingFormat[HdfsMessage[MongoOffset]] {
  override def transform(x: lifecycle.OplogClassifier): HdfsMessage[MongoOffset] = {
    val oplog = x.toOplog
    val tableName = oplog.getTableName
    val dbName = oplog.getDbName
    val docOption = getRealDoc(oplog)
    lazy val mongoOffset = MongoOffset(oplog.getTimestamp.getTime, oplog.getTimestamp.getInc)
    if (docOption.isEmpty) {
      HdfsMessage.abnormal(dbName, tableName, mongoOffset)
    } else {
      val doc = docOption.get
      val value = getJsonValue(doc)
      HdfsMessage(dbName, tableName, value, mongoOffset)
    }
  }
}
