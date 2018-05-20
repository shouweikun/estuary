package com.neighborhood.aka.laplace.estuary.core.task

import com.neighborhood.aka.laplace.estuary.bean.datasink.DataSinkBean
import com.neighborhood.aka.laplace.estuary.bean.resource.DataSourceBase
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection

/**
  * Created by john_liu on 2018/2/7.
  */
trait RecourceManager[sink, source <: DataSourceConnection, Sink <: SinkFunc] {

  val sinkBean: DataSinkBean
  val sourceBean: DataSourceBase
  /**
    * 数据源
    */
  lazy val source = buildSource
  /**
    * 数据汇
    */
  lazy val sink = buildSink

  /**
    * 构建数据源
    *
    * @return source
    */
  def buildSource: source

  /**
    * 构建数据汇
    *
    * @return sink
    */
  def buildSink: Sink
}
