package com.neighborhood.aka.laplace.estuary.bean.datasink

import com.neighborhood.aka.laplace.estuary.core.sink.hdfs.HdfsSinkFunc

/**
  * Created by john_liu on 2018/5/29.
  */
trait HdfsBean extends DataSinkBean[HdfsSinkFunc] {

  override def dataSinkType: String = SinkDataType.HDFS.toString

  def hdfsBasePath: String
}
