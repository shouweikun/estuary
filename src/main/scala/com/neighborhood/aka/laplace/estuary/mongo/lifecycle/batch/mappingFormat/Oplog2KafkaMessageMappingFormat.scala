package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.mappingFormat

import com.neighborhood.aka.laplace.estuary.bean.key.OplogKey
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoConnection
import org.bson.Document


/**
  * Created by john_liu on 2019/2/28.
  *
  * 将Oplog转化为kafkaMessage的MappingFormat
  *
  * @author neighborhood.aka.laplace
  */
final class Oplog2KafkaMessageMappingFormat(
                                             override val mongoConnection: MongoConnection,
                                             override val syncTaskId: String
                                           ) extends OplogMappingFormat[KafkaMessage] {

  /**
    *
    * @param x
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


  private def getJsonValue(doc: Document): String = {
    doc.toJson
  }
}
