package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch

import java.util.Calendar

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.akkaUtil.SyncDaemonCommand.ExternalSuspendTimedCommand
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.DataSourceFetcherPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{FetcherMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.{SourceManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt.OplogPowerAdapterCommand.OplogPowerAdapterUpdateCost
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.count.OplogProcessingCounterCommand.OplogProcessingCounterUpdateCount
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch.OplogFetcherCommand._
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch.OplogFetcherEvent.OplogFetcherActiveChecked
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoSourceManagerImp}
import com.neighborhood.aka.laplace.estuary.mongo.util.OplogOffsetHandler
import org.bson.BsonTimestamp
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.Try

/**
  * Created by john_liu on 2019/2/28.
  *
  * 處理oplog 拉取的actor
  *
  * sda专用
  *
  * @author neighborhood.aka.laplace
  */
final class SimpleOplogFetcher4sda(
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
  /**
    * 是否计数
    */
  val isCounting = taskManager.isCounting

  lazy val powerAdapter: Option[ActorRef] = taskManager.powerAdapter

  lazy val processingCounter: Option[ActorRef] = taskManager.processingCounter

  private val batchThreshold: Long = taskManager.batchThreshold

  private var delay: Long = 0
  private var lastFetchTimestamp = System.currentTimeMillis()

  private var suspendTs = getSuspendTs()

  /**
    * 位置处理器
    */
  private lazy val logPositionHandler: OplogOffsetHandler = taskManager.positionHandler
  /**
    * 拉取模块
    *
    * 在构建的时候有副作用，必须要把拿到的startPosition更新回去，不是一个好的设计
    */
  private lazy val simpleFetchModule: SimpleFetchModule = {
    val offset = logPositionHandler.findStartPosition(connection)
    logPositionHandler.persistLogPosition(syncTaskId, offset) //必须要更新回去，很重要！！！
    new SimpleFetchModule(connection, Option(offset), syncTaskId)
  }


  override def receive: Receive = {
    case FetcherMessage(OplogFetcherStart) => start
    case m@FetcherMessage(OplogFetcherFetch) => Try(handleFetchTask).failed.foreach(e => processError(e, m))
    case FetcherMessage(OplogFetcherUpdateDelay(x)) => delay = x
    case SyncControllerMessage(OplogFetcherUpdateDelay(x)) => delay = x
    case SyncControllerMessage(OplogFetcherStart) => start
    case FetcherMessage(OplogFetcherCheckActive) => sender() ! OplogFetcherActiveChecked()
    case OplogFetcherCheckActive => sender() ! OplogFetcherActiveChecked()
    case ExternalSuspendTimedCommand(_, ts) => handleUpdateHandleFetcherSuspendTimed(ts)
  }

  /**
    * 开始
    * 1.启动simpleFetchModule构建oplog拉取链接
    * 2.发送fetch命令
    */
  private def start: Unit = {
    log.info(s"OplogSimpleFetcher start,id:$syncTaskId")
    simpleFetchModule.start()
    sendFetchMessage(self, delay, OplogFetcherFetch)
  }

  /**
    * 处理更新挂起时间的方法
    *
    * @param ts 时间
    */
  private def handleUpdateHandleFetcherSuspendTimed(ts: Long): Unit = {
    log.info(s"update suspend ts:$ts,id:$syncTaskId")
    if (System.currentTimeMillis() > ts) switch2Suspend()
    else suspendTs = ts
  }

  /**
    * 处理拉取的核心方法
    *
    */
  private def handleFetchTask: Unit = {
    context.parent ! OplogFetcherBusy //忙碌
    sendFetchMessage(self, delay, OplogFetcherFetch) //发送下一次拉取的指令
    simpleFetchModule.fetch.fold(context.parent ! OplogFetcherFree) {
      doc =>

        val isSuspend = Option(doc.get("ts")).map(_.asInstanceOf[BsonTimestamp]).map { ts =>
          val re = ts.getTime > (suspendTs / 1000)
          //          log.info(s"$re,${ts.getTime}")
          re
        }.getOrElse(false) //判断是否需要挂起
        if (isSuspend) {
          switch2Suspend()
        } else {
          val curr = System.currentTimeMillis()
          sendData(doc)
          sendCost(System.currentTimeMillis() - lastFetchTimestamp)
          sendCount(1)
          lastFetchTimestamp = curr //更新一下时间
          context.parent ! OplogFetcherFree //空闲
        }
    }

  }

  private def switch2Suspend(): Unit = {
    log.info(s"time to suspend,ts:$suspendTs,id:$syncTaskId")
    context.become(suspend, true)
    context.parent ! OplogFetcherSuspend
  }

  /**
    * 发送数据
    *
    * @param data
    */
  private def sendData(data: Any) = downStream ! data

  /**
    * 计数
    *
    * @param count 数量
    * @return
    */
  private def sendCount(count: Long = 1l) = processingCounter.map(ref => ref ! FetcherMessage(OplogProcessingCounterUpdateCount(count)))

  /**
    * 计时
    *
    * @param cost
    * @return
    */
  private def sendCost(cost: Long = 1l) = powerAdapter.map(ref => ref ! FetcherMessage(OplogPowerAdapterUpdateCost(cost)))

  /**
    * 构建fetch命令
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

  /**
    * 获取挂起时间，默认午夜12点
    *
    * @return ts
    */
  private def getSuspendTs(): Long = {

    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_MONTH, 1)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.MILLISECOND, 0)
    val date = cal.getTime
    val re = date.getTime
    log.info(s"init suspend ts:$re,id:$syncTaskId")
    re
  }
}

object SimpleOplogFetcher4sda {
  val name = SimpleOplogFetcher4sda.getClass.getName.stripSuffix("$")

  def props(taskManager: MongoSourceManagerImp with TaskManager, downStream: ActorRef): Props = Props(new SimpleOplogFetcher4sda(taskManager, downStream))
}
