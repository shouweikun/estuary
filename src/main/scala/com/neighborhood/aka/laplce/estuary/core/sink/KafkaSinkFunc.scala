package com.neighborhood.aka.laplce.estuary.core.sink
import scala.concurrent.Future

/**
  * Created by john_liu on 2018/2/7.
  */
class KafkaSinkFunc extends SinkFunc{
  override def sink[A](source: A): Boolean = {
    ???
  }

  override def asyncSink[A](source: A): Future[Boolean] = {
    ???
  }
}
