package com.neighborhood.aka.laplce.estuary.core.task

import com.neighborhood.aka.laplce.estuary.core.sink.SinkFunc

/**
  * Created by john_liu on 2018/2/7.
  */
trait RecourceManager[sink,source,Sink<:SinkFunc[sink]] {

  def buildSource:source
  def buildSink:Sink
}
