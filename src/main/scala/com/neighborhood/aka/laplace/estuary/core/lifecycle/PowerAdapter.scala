package com.neighborhood.aka.laplace.estuary.core.lifecycle

/**
  * Created by john_liu on 2018/5/5.
  *
  * 所有传入的计算的时间的单位都是millsecond
  * 返回fetch的是微秒 microsecond
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
  var fetchTimestamp: Long = 0
  var fetchCountSum: Long = 0

  var batchTimeSum: Long = 0
  var batchTimestamp: Long = 0
  var batchCountSum: Long = 0

  var sinkTimeSum: Long = 0
  var sinkTimestamp: Long = 0
  var sinkCountSum: Long = 0

  /**
    * 通过时间戳方式更新fetch time
    *
    * @param timestamp
    */
  def updateFetchTimeByTimestamp(timestamp: Long) = {
    val nextFetchTimeWriteIndex = (fetchTimeWriteIndex + 1) % size
    fetchTimeArray(nextFetchTimeWriteIndex) = timestamp
    fetchTimeWriteIndex = nextFetchTimeWriteIndex
  }

  /**
    * 通过记录耗时的方式更新fetch time
    *
    * @param timeCost
    */
  def updateFetchTimeByTimeCost(timeCost: Long) = {
    val nextFetchTimeWriteIndex = (fetchTimeWriteIndex + 1) % size
    fetchTimeArray(nextFetchTimeWriteIndex) = timeCost
    fetchTimeWriteIndex = nextFetchTimeWriteIndex
  }

  /**
    * 通过时间戳方式更新Batch time
    *
    * @param timestamp
    */
  def updateSinkTimeByTimestamp(timestamp: Long) = {
    val nextFetchTimeWriteIndex = (batchTimeWriteIndex + 1) % size
    // 如果拿不到数据，默认在时间上随机增加3-5倍
    batchTimeArray(nextFetchTimeWriteIndex) = timestamp
    batchTimeWriteIndex = nextFetchTimeWriteIndex
  }

  /**
    * 通过记录耗时的方式更新sink time
    *
    * @param timeCost
    */
  def updateSinkTimeByTimeCost(timeCost: Long) = {
    val nextFetchTimeWriteIndex = (sinkTimeWriteIndex + 1) % size
    // 如果拿不到数据，默认在时间上随机增加3-5倍
    sinkTimeArray(nextFetchTimeWriteIndex) = timeCost
    sinkTimeWriteIndex = nextFetchTimeWriteIndex
  }

  /**
    * 通过时间戳方式更新Batch time
    *
    * @param timestamp
    */
  def updateBatchTimeByTimestamp(timestamp: Long) = {
    val nextFetchTimeWriteIndex = (sinkTimeWriteIndex + 1) % size
    // 如果拿不到数据，默认在时间上随机增加3-5倍
    sinkTimeArray(nextFetchTimeWriteIndex) = timestamp
    sinkTimeWriteIndex = nextFetchTimeWriteIndex
  }

  /**
    * 通过记录耗时的方式更新batch time
    *
    * @param timeCost
    */
  def updateBatchTimeByTimeCost(timeCost: Long) = {
    val nextFetchTimeWriteIndex = (batchTimeWriteIndex + 1) % size
    batchTimeArray(nextFetchTimeWriteIndex) = timeCost
    batchTimeWriteIndex = nextFetchTimeWriteIndex
  }

  protected def computeCostByTimeCost(timeArray: Array[Long]): Long = {
    (fetchTimeArray.fold(0L)(_ + _))./(size)
  }

  protected def computeCostByTimestamp(timeArray: Array[Long], index: Int): Long = {
    timeArray(index) - timeArray((index + 1) % size) / size
  }

  /**
    *
    * @param sum          计算的耗时 ms
    * @param timeInterVal 时间间隔 s
    * @return
    */
  protected def computeCostPercentage(sum: Long, timeInterVal: Long) = sum / timeInterVal / 10
  protected def
  /**
    * 计算cost
    */
  def computeCost: Unit

  /**
    * 进行控制
    */
  def control: Unit
}
