package com.neighborhood.aka.laplace.estuary.core.lifecycle.worker

import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

import scala.util.Try

/**
  * Created by john_liu on 2018/5/6.
  */
trait ProcessingCounter {
  def taskManager: TaskManager

  protected var fetchCount: Long = 0
  protected var batchCount: Long = 0
  protected var sinkCount: Long = 0

  protected var lastFetchCount: Long = 0
  protected var lastBatchCount: Long = 0
  protected var lastSinkCount: Long = 0

  protected var fetchCharge = 0l
  protected var batchCharge = 0l
  protected var sinkCharge = 0l

  def addFetchCount(inc: Long = 1) = {
    fetchCount = fetchCount + inc
  }

  def addBatchCount(inc: Long = 1) = {
    batchCount = batchCount + inc
  }

  def addSinkCount(inc: Long = 1) = {
    sinkCount = sinkCount + inc
  }

  /**
    *
    * @param interval 间隔，单位是毫秒
    */
  def updateRecord(interval: Long): Unit = {

    if (fetchCount - lastFetchCount > 0) {
      taskManager.fetchCount.set(fetchCount)
      val value = Try((fetchCount - lastFetchCount) * 1000 / interval / math.max(fetchCharge,1)).getOrElse(1l)
      taskManager.fetchCountPerSecond.set(value)
      lastFetchCount = fetchCount
      fetchCharge = 0
    } else fetchCharge = fetchCharge + 1
    if (batchCount - lastBatchCount > 0) {
      taskManager.batchCount.set(batchCount)
      val value = Try((batchCount - lastBatchCount) * 1000 / interval / math.max(1,batchCharge)).getOrElse(1l)
      taskManager.batchCountPerSecond.set(value)
      lastBatchCount = batchCount
      batchCharge = 0
    } else batchCharge = batchCharge + 1
    if (sinkCount - lastSinkCount > 0) {
      taskManager.sinkCount.set(sinkCount)
      val value = Try((sinkCount - lastSinkCount) * 1000 / interval / math.max(1,sinkCharge)).getOrElse(1l)
      taskManager.sinkCountPerSecond.set(value)
      sinkCharge = 0
      lastSinkCount = sinkCount
    } else sinkCharge = sinkCharge + 1


  }


}
