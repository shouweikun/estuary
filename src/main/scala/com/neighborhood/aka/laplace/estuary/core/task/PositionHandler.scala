package com.neighborhood.aka.laplace.estuary.core.task

import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection

/**
  *
  * @tparam A log类型
  */
trait PositionHandler[A] {

  def start():Unit

  def isStart: Boolean

  def close(): Unit

  def persistLogPosition(destination:String,logPosition:A):Unit

  def getlatestIndexBy(destination:String):A

  def findStartPosition(conn:DataSourceConnection):A
}
