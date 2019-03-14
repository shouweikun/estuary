package com.neighborhood.aka.laplace.estuary.bean.datasink

import com.neighborhood.aka.laplace.estuary.core.sink.hbase.HBaseSinkFunc

/**
  * Created by john_liu on 2018/6/1.
  *
  */

trait HBaseBean extends DataSinkBean[HBaseSinkFunc] {


  def HbaseZookeeperQuorum: String

  def HabseZookeeperPropertyClientPort: String

  override val dataSinkType: String = SinkDataType.HBASE.toString
}
