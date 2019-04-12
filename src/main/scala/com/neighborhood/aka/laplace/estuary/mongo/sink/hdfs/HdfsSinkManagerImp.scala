package com.neighborhood.aka.laplace.estuary.mongo.sink.hdfs

import java.util.concurrent.atomic.AtomicBoolean

import com.neighborhood.aka.laplace.estuary.core.sink.hdfs.HdfsSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.SinkManager

/**
  * Created by john_liu on 2019/4/12.
  */
trait HdfsSinkManagerImp extends SinkManager[HdfsSinkFunc] {
  /**
    * 构建数据汇
    *
    * @return sink
    */
  override def buildSink: HdfsSinkFunc = {
    new HdfsSinkImp
  }

  lazy val sinkAbnormal = new AtomicBoolean(false)
}
