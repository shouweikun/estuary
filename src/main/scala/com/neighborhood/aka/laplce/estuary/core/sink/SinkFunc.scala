package com.neighborhood.aka.laplce.estuary.core.sink

import scala.concurrent.Future

/**
  * Created by john_liu on 2018/2/7.
  */
trait SinkFunc {
  def sink[Source](source: Source): Boolean

  def asyncSink[Source](source: Source):Future[Boolean]
}
