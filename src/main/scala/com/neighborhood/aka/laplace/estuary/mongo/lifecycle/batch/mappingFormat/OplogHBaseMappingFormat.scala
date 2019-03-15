package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.mappingFormat

import com.neighborhood.aka.laplace.estuary.bean.support.HBasePut
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoOffset}

/**
  * Created by john_liu on 2019/3/15.
  */
class OplogHBaseMappingFormat extends OplogMappingFormat[HBasePut[MongoOffset]] {
  override def syncTaskId: String = ???

  /**
    * mongoConnection 用于u事件反查
    *
    * @return
    */
  override def mongoConnection: MongoConnection = ???

  override def transform(x: lifecycle.OplogClassifier): HBasePut[MongoOffset] = {
    val oplog = x.toOplog
    val docOption = getRealDoc(oplog)
    if (docOption.isEmpty)
  }
}
