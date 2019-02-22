package com.neighborhood.aka.laplace.estuary.core.task

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.bean.identity.BaseExtractBean
import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.WorkerType.WorkerType
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.{Status, WorkerType}
import com.neighborhood.aka.laplace.estuary.core.snapshot.SnapshotStatus
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat
import com.typesafe.config.Config

import scala.annotation.tailrec
import scala.util.Try

/**
  * Created by john_liu on 2018/2/7.
  * 负责管理资源和任务
  */
trait TaskManager {
  /**
    * batch转换模块
    */
  def batchMappingFormat: Option[MappingFormat[_, _]]

  /**
    * 事件溯源的事件收集器
    */
  def eventCollector: Option[ActorRef]

  /**
    * 任务信息bean
    */
  def taskInfo: BaseExtractBean

  /**
    * 传入的配置
    *
    * @return
    */
  def config: Config

  /**
    * 是否计数，默认不计数
    */
  def isCounting: Boolean

  /**
    * 是否计算每条数据的时间，默认不计时
    */
  def isCosting: Boolean

  /**
    * 是否保留最新binlog位置
    */
  def isProfiling: Boolean

  /**
    * 是否打开功率调节器
    */
  def isPowerAdapted: Boolean

  /**
    * 是否同步写
    */
  def isSync: Boolean

  /**
    * 是否是补录任务
    */
  def isDataRemedy: Boolean

  //
  //  /**
  //    * 监听心跳用的语句
  //    */
  //  abstract def delectingCommand: String
  //
  //  /**
  //    * 监听重试次数标准值
  //    */
  //  def listeningRetryTimeThreshold: Int

  /**
    * 任务开始时间
    */
  /**
    * 任务类型
    * 由三部分组成
    * DataSourceType-DataSyncType-DataSinkType
    */
  def taskType: String


  /**
    * 分区模式
    *
    * @return
    */
  def partitionStrategy: PartitionStrategy

  /**
    * 是否阻塞式拉取
    *
    * @return
    */
  def isBlockingFetch: Boolean

  /**
    * 同步任务开始时间 用于fetch过滤无用字段
    *
    * @return
    */
  def syncStartTime: Long

  //
  //  /**
  //    * 是否开启事件溯源
  //    */
  //  def AllEventOn: Boolean

  /**
    * 最大消息差
    *
    * @return
    */
  def maxMessageGap: Long = Try(config.getLong("estuary.max.message.diff")).getOrElse(10000l)

  /**
    * 加载的fetcherName
    */
  def fetcherNameToLoad: Map[String, String] = Map.empty

  /**
    * 加载的sinker的名称
    */
  def sinkerNameToLoad: Map[String, String] = Map.empty

  /**
    * 加载的controller的名称
    */
  def controllerNameToLoad: Map[String, String] = Map.empty

  /**
    * 加载的batcher的名称
    */
  def batcherNameToLoad: Map[String, String] = Map.empty

  /**
    * 是否发送心跳
    */
  def isSendingHeartbeat: Boolean = true

  /**
    * 初始化/启动
    */
  def start: Unit = {

  }

  /**
    * 关闭
    * 当与资源管理器eg:SinkManager和SourceManager绑定时，将资源关闭交给这个方法
    */
  def close: Unit = {}

  /**
    * 计数器的引用
    */
  @volatile var processingCounter: Option[ActorRef] = None
  /**
    * 功率控制器的引用
    *
    * @return
    */
  @volatile var powerAdapter: Option[ActorRef] = None
  /**
    * position记录器的引用
    */
  @volatile var positionRecorder: Option[ActorRef]
  = None
  /**
    * sinkerList
    *
    * @return
    */
  @volatile var sinkerList: List[ActorRef] = Nil
  /**
    * fetcher的状态
    */
  var fetcherStatus: AtomicReference[Status] = new AtomicReference[Status](Status.OFFLINE)
  /**
    * batcher的状态
    */
  var batcherStatus: AtomicReference[Status] = new AtomicReference[Status](Status.OFFLINE)
  /**
    * heartbeatListener的状态
    */
  var heartBeatListenerStatus: AtomicReference[Status] = new AtomicReference[Status](Status.OFFLINE)
  /**
    * sinker的状态
    */

  val sinkerStatus: AtomicReference[Status] = new AtomicReference[Status](Status.OFFLINE)
  /**
    * syncControllerStatus的状态
    */
  val syncControllerStatus: AtomicReference[Status] = new AtomicReference[Status](Status.OFFLINE)

  /**
    * 同步任务标识
    */
  def syncTaskId: String

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
    * 数据处理时间占比
    */
  lazy val fetchCostPercentage = new AtomicLong(0)
  lazy val batchCostPercentage = new AtomicLong(0)
  lazy val sinkCostPercentage = new AtomicLong(0)
  /**
    * 每秒处理数据
    */
  lazy val fetchCountPerSecond = new AtomicLong(0)
  lazy val batchCountPerSecond = new AtomicLong(0)
  lazy val sinkCountPerSecond = new AtomicLong(0)

  /**
    * 数据处理时间记录
    */
  def sinkerLogPosition: AtomicReference[String] = sinkerLogPosition_

  lazy val sinkerLogPosition_ = new AtomicReference[String]("")
  /**
    * 快照任务状态
    */
  lazy val snapshotStauts = new AtomicReference[String](
    s"""
    {
       syncTaskId:"$syncTaskId",
       status:"${SnapshotStatus.UNKNOWN}"
    }
    """.stripMargin)

  /**
    * 拉取数据时延
    */
  lazy val fetchDelay: AtomicLong = new AtomicLong(0)

  /**
    * 打包阈值
    */
  def batchThreshold: Long

  /**
    * batcher的数量
    */
  def batcherNum: Int

  /**
    * sinker的数量
    */
  def sinkerNum: Int

  /**
    * 任务运行状态
    * 此trait的实现类可以扩展此方法返回具体部件的状态
    */
  def taskStatus: Map[String, Status] = {
    val thisSyncControllerStatus = syncControllerStatus.get()
    val thisHeartBeatListenerStatus = heartBeatListenerStatus.get()
    val thisBatcherStatus = batcherStatus.get()
    val thisSinkerStatus = sinkerStatus.get()
    val thisFetcherStatus = fetcherStatus.get()
    Map(
      "controllerStatus" -> thisSyncControllerStatus,
      "heartbeatListenerStatus" -> thisHeartBeatListenerStatus,
      "batcerStatus" -> thisBatcherStatus,
      "sinkerStatus" -> thisSinkerStatus,
      "fetcherStatus" -> thisFetcherStatus
    )
  }

  /**
    * 记录count，输出count相关监控信息
    *
    * @return
    */
  def logCount: Map[String, Long] = {
    lazy val fetchCount = this.fetchCount.get()
    lazy val batchCount = this.batchCount.get()
    lazy val sinkCount = this.sinkCount.get()
    lazy val fetchCountPerSecond = this.fetchCountPerSecond.get()
    lazy val batchCountPerSecond = this.batchCountPerSecond.get()
    lazy val sinkCountPerSecond = this.sinkCountPerSecond.get()
    Map("sinkCount" -> sinkCount, "batchCount" -> batchCount, "fetchCount" -> fetchCount, "fetchCountPerSecond" -> fetchCountPerSecond, "batchCountPerSecond" -> batchCountPerSecond, "sinkCountPerSecond" -> sinkCountPerSecond)

  }

  /**
    * 记录耗时，输出耗时信息
    *
    * @return
    */
  def logTimeCost: Map[String, Long] = {
    lazy val fetchCost = this.fetchCost.get()
    lazy val batchCost = this.batchCost.get()
    lazy val sinkCost = this.sinkCost.get()
    lazy val fetchCostPercentage = this.fetchCostPercentage.get()
    lazy val batchCostPercentage = this.batchCostPercentage.get()
    lazy val sinkCostPercentage = this.sinkCostPercentage.get()
    Map("fetchCost" -> fetchCost, "batchCost" -> batchCost, "sinkCost" -> sinkCost, "fetchCostPercentage" -> fetchCostPercentage, "batchCostPercentage" -> batchCostPercentage, "sinkCostPercentage" -> sinkCostPercentage)

  }

  /**
    * 等待sinkCount == fetchCount == batchCount
    * 默认等待一分钟
    */
  @tailrec
  final def wait4TheSameCount(startTime: Long = System.currentTimeMillis()): Unit = {
    if (System.currentTimeMillis() - startTime > 60000) throw new RuntimeException(s"1 min has passed but still not same count,id:$syncTaskId")
    if (!sameCountOnce) {
      Thread.sleep(50)
      wait4TheSameCount(startTime)
    }
  }

  @inline
  private def sameCountOnce: Boolean = {
    lazy val theFetchCount = this.fetchCount.get()
    lazy val theBatchCount = this.batchCount.get()
    lazy val theSinkCount = this.sinkCount.get()
    theFetchCount == theBatchCount && theBatchCount == theSinkCount
  }

  /**
    * 等待sinkerList是否更新完成
    *
    * @since 2019-01-14
    */
  @tailrec
  final def wait4SinkerList(startTime: Long = System.currentTimeMillis()): Unit = if (this.sinkerList.isEmpty) {
    if (System.currentTimeMillis() - startTime > 60000) throw new RuntimeException(s"1 min has passed but sinkerList still empty,id:$syncTaskId")
    wait4SinkerList(startTime)
  }


}

object TaskManager {
  private lazy val taskStatusMap: ConcurrentHashMap[String, Map[String, Status.Status]] = new ConcurrentHashMap[String, Map[String, Status.Status]]()
  private lazy val taskManagerMap: ConcurrentHashMap[String, TaskManager] = new ConcurrentHashMap[String, TaskManager]()

  /**
    * 状态变化
    */
  def changeFunc(status: Status, taskManager: TaskManager)(implicit workerType: WorkerType): Unit = {
    workerType match {
      case WorkerType.Listener => taskManager.heartBeatListenerStatus.set(status)
      case WorkerType.Batcher => taskManager.batcherStatus.set(status)
      case WorkerType.Sinker => taskManager.sinkerStatus.set(status)
      case WorkerType.Fetcher => taskManager.fetcherStatus.set(status)
      case WorkerType.SyncController => taskManager.syncControllerStatus.set(status)
    }
  }

  /**
    * 状态变化管理
    */
  def changeStatus(status: Status, changFunc: Status => Unit, onChangeFunc: => Unit): Unit = {
    changFunc(status)
    onChangeFunc
  }

  /**
    * 每当任务状态变化时，更新之
    */
  def onChangeStatus(taskManager: TaskManager): Unit = {
    val syncTaskId = taskManager.syncTaskId
    val syncControllerStatus = taskManager.syncControllerStatus.get
    val fetcherStatus = taskManager.fetcherStatus.get
    val sinkerStatus = taskManager.sinkerStatus.get
    val batcherStatus = taskManager.batcherStatus.get
    val listenerStatus = taskManager.heartBeatListenerStatus.get
    val map = Map("syncControllerStatus" -> syncControllerStatus, "fetcherStatus" -> fetcherStatus, "sinkerStatus" -> sinkerStatus, "batcherStatus" -> batcherStatus, "listenerStatus" -> listenerStatus)

    taskStatusMap.put(syncTaskId, map)
  }

  /**
    * 移除TaskManager
    *
    * @param name SyncTaskId
    */
  def removeTaskManager(name: String): Unit = taskManagerMap.remove(name)

  /**
    * 存放 taskManager
    *
    * @param name syncTaskId
    * @param taskManager
    */
  def putTaskManager(name: String, taskManager: TaskManager): Unit = taskManagerMap.put(name, taskManager)

  /**
    * 获取TaskManager
    *
    * @param name SyncTaskId
    * @return Option
    */
  def getTaskManager(name: String): Option[TaskManager] = Option(taskManagerMap.get(name))

}
