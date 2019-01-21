package com.neighborhood.aka.laplace.estuary.core.offset

import scala.util.Try

/**
  * Created by john_liu on 2018/7/24.
  */
trait LogPositionManager[T] {

  def start: Try[Unit]

  def stop: Try[Unit]

  def getLatestIndexBy(destination: String): T

  def persistLogPosition(destination: String, logPosition: T): Unit
}
