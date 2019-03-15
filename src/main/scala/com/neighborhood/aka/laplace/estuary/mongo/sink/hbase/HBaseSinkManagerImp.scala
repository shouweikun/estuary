package com.neighborhood.aka.laplace.estuary.mongo.sink.hbase

import com.neighborhood.aka.laplace.estuary.core.sink.hbase.HBaseSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.SinkManager

/**
  * Created by john_liu on 2019/3/15.
  */
trait HBaseSinkManagerImp extends SinkManager[HBaseSinkFunc] {


  /**
    * 数据汇bean
    */
  override def sinkBean: HBaseBeanImp

  /**
    * 构建数据汇
    *
    * @return sink
    */
  override def buildSink: HBaseSinkFunc = {
    new HBaseSinkImp(sinkBean)
  }
}
