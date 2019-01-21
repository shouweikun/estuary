package com.neighborhood.aka.laplace.estuary.core.task

import com.neighborhood.aka.laplace.estuary.bean.datasink.DataSinkBean
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc

import scala.util.Try

/**
  * Created by john_liu on 2019/1/13.
  */
trait SinkManager[S <: SinkFunc] {

  /**
    * 数据汇bean
    */
   def sinkBean: DataSinkBean[S]


  /**
    * 数据汇
    */
  def sink:S = sink_

  private lazy val sink_ :S= buildSink

  /**
    * 构建数据汇
    *
    * @return sink
    */
   def buildSink: S

  /**
    * 统一关闭资源
    */
  def closeSink:Unit = Try{
    if(!sink.isTerminated)sink.close
  }



}
