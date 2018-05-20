package com.neighborhood.aka.laplace.estuary.core.task

import com.neighborhood.aka.laplace.estuary.bean.datasink.DataSinkBean
import com.neighborhood.aka.laplace.estuary.bean.resource.DataSourceBase
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc

/**
  * Created by john_liu on 2018/2/7.
  */
trait RecourceManager[sink,source,Sink<:SinkFunc] {

  val sinkBean:DataSinkBean
  val sourceBean:DataSourceBase
  def buildSource:source
  def buildSink:Sink
}
