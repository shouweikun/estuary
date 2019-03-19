package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.mappingFormat

import com.neighborhood.aka.laplace.estuary.bean.key.OplogKey
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoConnection
import com.neighborhood.aka.laplace.estuary.mongo.util.MongoDocumentToJson
import org.bson.Document


/**
  * Created by john_liu on 2019/2/28.
  *
  * 将Oplog转化为kafkaMessage的MappingFormat
  *
  * @note 不负责mongoConnection的生命周期
  * @author neighborhood.aka.laplace
  */
final class Oplog2KafkaMessageMappingFormat(
                                             override val mongoConnection: MongoConnection, //mongo链接
                                             override val syncTaskId: String,
                                             override val mongoDocumentToJson: MongoDocumentToJson
                                           ) extends OplogMappingFormat[KafkaMessage] {

  /**
    * 将oplogClassifier装换成KafkaMessage
    *
    * @param x OplogClassifier
    * @return
    */
  override def transform(x: lifecycle.OplogClassifier): KafkaMessage = {
    val oplog = x.toOplog
    val oplogKey = new OplogKey(oplog)
    val docOption = getRealDoc(oplog)
    if (docOption.isEmpty) KafkaMessage.abnormal(oplogKey)
    else {
      val doc = docOption.get
      if (doc.get("_id") == null) KafkaMessage.abnormal(oplogKey)
      else {
        val json = getJsonValue(doc)
        KafkaMessage(oplogKey, json)
      }
    }

  }

  @inline
  private def getJsonValue(doc: Document): String = {
    doc.toJson
  }
}
