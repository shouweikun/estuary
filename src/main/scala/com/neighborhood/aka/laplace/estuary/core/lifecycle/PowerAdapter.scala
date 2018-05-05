package com.neighborhood.aka.laplace.estuary.core.lifecycle

/**
  * Created by john_liu on 2018/5/5.
  */
trait PowerAdapter {


  val size: Int = 10
  var fetchTimeArray: Array[Long] = new Array[Long](size)
  var batchTimeArray: Array[Long] = new Array[Long](size)
  var sinkTimeArray: Array[Long] = new Array[Long](size)

  var fetchTimeWriteIndex: Int = 0
  var batchTimeWriteIndex: Int = 0
  var sinkTimeWriteIndex: Int = 0

  var fetchTimeSum: Long = 0
  var fetchCountSum: Long = 0

  var batchTimeSum: Long = 0
  var batchCountSum: Long = 0

  var sinkTimeSum: Long = 0
  var sinkCountSum: Long = 0

  /**
    * 计算cost
    */
  def computeCost: Unit

  /**
    * 进行控制
    */
  def control: Unit
}
