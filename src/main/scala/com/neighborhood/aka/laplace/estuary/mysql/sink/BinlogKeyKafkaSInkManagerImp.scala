package com.neighborhood.aka.laplace.estuary.mysql.sink

import com.neighborhood.aka.laplace.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.SinkManager
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2019/2/26.
  */
trait BinlogKeyKafkaSInkManagerImp extends SinkManager[KafkaSinkFunc[BinlogKey, String]] {
  override protected lazy val logger = LoggerFactory.getLogger(classOf[BinlogKeyKafkaSInkManagerImp])

  /**
    * 数据汇bean
    */
  def sinkBean: BinlogKeyKafkaBeanImp

  /**
    * 构建数据汇
    *
    * @return sink
    */
  override def buildSink: BinlogKeyKafkaSinkFunc = {
    new BinlogKeyKafkaSinkFunc(sinkBean)
  }
}
