package com.neighborhood.aka.laplace.estuary.core.task

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import com.neighborhood.aka.laplace.estuary.core.lifecycle.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.WorkerType.WorkerType
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{Status, WorkerType}

/**
  * Created by john_liu on 2018/2/7.
  * 负责管理资源和任务
  */
trait TaskManager {
  /**
    * 任务类型
    * 由三部分组成
    * DataSourceType-DataSyncType-DataSinkType
    */
  def taskType: String

  /**
    * fetcher的状态
    */
  @volatile
  var fetcherStatus: Status = Status.OFFLINE
  /**
    * batcher的状态
    */
  @volatile
  var batcherStatus: Status = Status.OFFLINE
  /**
    * heartbeatListener的状态
    */
  @volatile
  var heartBeatListenerStatus: Status = Status.OFFLINE
  /**
    * sinker的状态
    */
  @volatile
  var sinkerStatus: Status = Status.OFFLINE
  /**
    * syncControllerStatus的状态
    */
  @volatile
  var syncControllerStatus: Status = Status.OFFLINE

  /**
    * 数据条目记录
    */
  lazy val fetchCount = new AtomicLong(0)
  lazy val batchCount = new AtomicLong(0)
  lazy val sinkCount = new AtomicLong(0)

  /**
    * 数据处理时间记录
    */
  lazy val fetchCost = new AtomicLong(0)
  lazy val batchCost = new AtomicLong(0)
  lazy val sinkCost = new AtomicLong(0)
  /**
    * 数据处理时间记录
    */
  lazy val sinkerLogPosition = new AtomicReference[String]("")
  /**
    * 拉取数据时延
    */
  lazy val fetchDelay: AtomicLong = null
  /**
    * 打包阈值
    */
  lazy val batchThreshold: AtomicLong = null

  /**
    * 任务运行状态
    * 此trait的实现类可以扩展此方法返回具体部件的状态
    */
  def taskStatus: Map[String, Status] = {
    val thisTaskStatus = syncControllerStatus
    Map("task" -> thisTaskStatus)
  }

}

object TaskManager {
  /**
    * 状态变化
    */
  def changeFunc(status: Status, taskManager: TaskManager)(implicit workerType: WorkerType): Unit = {
    workerType match {
      case WorkerType.Listener => taskManager.heartBeatListenerStatus = status
      case WorkerType.Batcher => taskManager.batcherStatus = status
      case WorkerType.Sinker => taskManager.sinkerStatus = status
      case WorkerType.Fetcher => taskManager.fetcherStatus = status
      case WorkerType.SyncController => taskManager.syncControllerStatus = status
    }
  }

  /**
    * 状态变化管理
    */
  def changeStatus(status: Status, changFunc: Status => Unit, onChangeFunc: => Unit): Unit = {
    changFunc(status)
    onChangeFunc
  }
}
