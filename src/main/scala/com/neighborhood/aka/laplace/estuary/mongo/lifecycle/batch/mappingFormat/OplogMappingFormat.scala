package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.mappingFormat

import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.OplogClassifier
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, Oplog}
import com.neighborhood.aka.laplace.estuary.mongo.util.MongoDocumentToJson
import org.bson.Document
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2019/2/28.
  *
  * @author neighborhood.aka.laplace
  */
trait OplogMappingFormat[B] extends MappingFormat[OplogClassifier, B] {
  protected lazy val logger = LoggerFactory.getLogger(classOf[OplogMappingFormat[B]])

  def mongoDocumentToJson: MongoDocumentToJson

  def isBson: Boolean = true

  def syncTaskId: String

  /**
    * mongoConnection 用于u事件反查
    *
    * @return
    */
  def mongoConnection: MongoConnection

  /**
    * 获取有效doc
    *
    * @param oplog oplog
    * @return 找到Some(doc) else None
    */
  protected def getRealDoc(oplog: Oplog): Option[Document] = {
    if (oplog.getOperateType == "u") mongoConnection.findRealDocForUpdate(oplog)
    else Option(oplog.getCurrentDocument)
  }

  protected def getJsonValue(o: Document): String = {
    if (isBson) mongoDocumentToJson.docToBson(o) else mongoDocumentToJson.docToJson(o)
  }
}
