package com.neighborhood.aka.laplace.estuary.mongo.sink

import com.neighborhood.aka.laplace.estuary.bean.key.{BinlogKey, OplogKey}
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.SinkManager
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2019/2/26.
  */
trait OplogKeyKafkaSinkManagerImp extends SinkManager[KafkaSinkFunc[OplogKey, String]] {
  override protected lazy val logger = LoggerFactory.getLogger(classOf[OplogKeyKafkaSinkManagerImp])

  /**
    * 数据汇bean
    */
  override def sinkBean: OplogKeyKafkaBeanImp

  /**
    * 构建数据汇
    *
    * @return sink
    */
  override def buildSink: OplogKeyKafkaSinkFunc = {
    new OplogKeyKafkaSinkFunc(sinkBean)
  }
}
