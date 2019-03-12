package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.DataSourceFetcherPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, FetcherMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.{SourceManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt.OplogPowerAdapterCommand.OplogPowerAdapterUpdateCost
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.count.OplogProcessingCounterCommand.OplogProcessingCounterUpdateCount
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch.OplogFetcherCommand._
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoSourceManagerImp}
import com.neighborhood.aka.laplace.estuary.mongo.util.OplogOffsetHandler

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.Try

/**
  * Created by john_liu on 2019/2/28.
  *
  * 處理oplog 拉取的actor
  *
  * @author neighborhood.aka.laplace
  */
final class SimpleOplogFetcher(
                                val taskManager: MongoSourceManagerImp with TaskManager,
                                val downStream: ActorRef
                              ) extends DataSourceFetcherPrototype[MongoConnection] {

  implicit val transTaskPool: ExecutionContextExecutor = scala.concurrent.ExecutionContext.Implicits.global //使用广域线程池
  /**
    * 数据源资源管理器
    */
  override val sourceManager: SourceManager[MongoConnection] = taskManager

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

  val isCounting = taskManager.isCounting

  lazy val powerAdapter = taskManager.powerAdapter

  lazy val processingCounter = taskManager.processingCounter

  val batchNum: Long = taskManager.batchThreshold

  private var delay: Long = 0
  private var lastFetchTimestamp = System.currentTimeMillis()
  /**
    * 位置处理器
    */
  private lazy val logPositionHandler: OplogOffsetHandler = taskManager.positionHandler
  /**
    * 拉取模块
    */
  private lazy val simpleFetchModule: SimpleFetchModule = {
    val offset = Option(logPositionHandler.findStartPosition(connection))
    new SimpleFetchModule(connection, offset, syncTaskId)
  }


  override def receive: Receive = {
    case FetcherMessage(OplogFetcherStart) => start
    case m@FetcherMessage(OplogFetcherFetch) => Try(handleFetchTask).failed.foreach(e => processError(e, m))
    case FetcherMessage(OplogFetcherUpdateDelay(x)) => delay = x
    case SyncControllerMessage(OplogFetcherUpdateDelay(x)) => delay = x
    case SyncControllerMessage(OplogFetcherStart) => start
  }

  private def start: Unit = {
    log.info(s"OplogSimpleFetcher start,id:$syncTaskId")
    simpleFetchModule.start()
    sendFetchMessage(self, delay, OplogFetcherFetch)
  }

  /**
    * 处理拉取的核心方法
    *
    */
  private def handleFetchTask: Unit = {
    sendFetchMessage(self, delay, OplogFetcherFetch) //发送下一次拉取的指令
    simpleFetchModule.fetch.foreach {
      doc =>
        val curr = System.currentTimeMillis()
        sendData(doc)
        sendCost(System.currentTimeMillis() - lastFetchTimestamp)
        sendCount(1)
        lastFetchTimestamp = curr //更新一下时间
    }


  }

  private def sendData(data: Any) = downStream ! data

  private def sendCount(count: Long = 1l) = processingCounter.map(ref => ref ! FetcherMessage(OplogProcessingCounterUpdateCount(count)))

  private def sendCost(cost: Long = 1l) = powerAdapter.map(ref => ref ! FetcherMessage(OplogPowerAdapterUpdateCost(cost)))

  /**
    *
    * @param ref
    * @param delay
    * @param message
    */
  private def sendFetchMessage(ref: ActorRef = self, delay: Long, message: Any): Unit = {
    lazy val fetchMessage = FetcherMessage(message)
    delay match {
      case x if (x <= 0) => ref ! fetchMessage
      case _ => context.system.scheduler.scheduleOnce(delay microseconds, self, fetchMessage)
    }
  }


  /**
    * 错位次数阈值
    */
  override def errorCountThreshold: Int = 0

  /**
    * 错位次数
    */
  override var errorCount: Int = 0

  /**
    * 错误处理
    *
    * oplog版的不支持重试
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {
    log.error(s"oplog simple fetcher crashe,cause:$e,message:${e.getMessage},id:$syncTaskId")
    e.printStackTrace()
    throw e
  }
}

object SimpleOplogFetcher {
  val name = SimpleOplogFetcher.getClass.getName.stripSuffix("$")

  def props(taskManager: MongoSourceManagerImp with TaskManager, downStream: ActorRef): Props = Props(new SimpleOplogFetcher(taskManager, downStream))
}