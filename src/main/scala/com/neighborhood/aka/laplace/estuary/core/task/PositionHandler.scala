package com.neighborhood.aka.laplace.estuary.core.task

import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import org.slf4j.LoggerFactory

/**
  *
  * @tparam A log类型
  */
trait PositionHandler[A] {

  protected lazy val logger = LoggerFactory.getLogger(classOf[PositionHandler[A]])

  def start():Unit

  def isStart: Boolean

  def close(): Unit

  def persistLogPosition(destination:String,logPosition:A):Unit

  def getlatestIndexBy(destination:String):A

  def findStartPosition(conn:DataSourceConnection):A
}
