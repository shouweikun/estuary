package com.neighborhood.aka.laplace.estuary.core.sink.hbase

import com.neighborhood.aka.laplace.estuary.bean.datasink.{DataSinkBean, HBaseBean}
import com.neighborhood.aka.laplace.estuary.core.task.SinkManager

/**
  * Created by john_liu on 2019/3/14.
  */
trait HBaseSinkManager extends SinkManager[HBaseSinkFunc] {


  /**
    * 数据汇bean
    */
  override def sinkBean: HBaseBean


}
