package com.neighborhood.aka.laplace.estuary.core.lifecycle.worker

import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2018/5/6.
  */
trait ProcessingCounter {
  val taskManager: TaskManager

  var fetchCount: Long = 0
  var batchCount: Long = 0
  var sinkCount: Long = 0

  def addFetchCount(inc: Long = 1) = {
    fetchCount = fetchCount + inc
  }

  def addBatchCount(inc: Long = 1) = {
    batchCount = batchCount + inc
  }

  def addSinkCount(inc: Long = 1) = {
    sinkCount = sinkCount + inc
  }

  def updateRecord: Unit = {
    taskManager.fetchCount.set(fetchCount)
    taskManager.batchCount.set(batchCount)
    taskManager.sinkCount.set(sinkCount)
  }
}
