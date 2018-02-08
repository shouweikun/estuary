package com.neighborhood.aka.laplce.estuary.core.task

import com.neighborhood.aka.laplce.estuary.core.sink.SinkFunc

/**
  * Created by john_liu on 2018/2/7.
  */
trait RecourceManager[Source,Sink<:SinkFunc] {

  def buildSource:Source
  def buildSink:Sink
}
