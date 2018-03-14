package com.neighborhood.aka.laplace.estuary.core.task

import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc

/**
  * Created by john_liu on 2018/2/7.
  */
trait RecourceManager[sink,source,Sink<:SinkFunc] {

  def buildSource:source
  def buildSink:Sink
}
