package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataFetcherManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{FetcherMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mongo.SettingConstant
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch.OplogFetcherCommand._
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch.OplogFetcherEvent.OplogFetcherActiveChecked
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoSourceManagerImp

import scala.concurrent.duration._

/**
  * Created by john_liu on 2019/3/3.
  *
  * @author neighborhood.aka.laplace
  * @note
  */
final class OplogFetcherManager(
                                 override val taskManager: TaskManager,
                                 override val batcher: ActorRef
                               ) extends SourceDataFetcherManagerPrototype {

  implicit val ec = context.dispatcher

  /**
    * 是否是最上层的manager
    */
  override def isHead: Boolean = true

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

  /**
    * 上一次活跃的时间
    */
  private var lastActive: Long = System.currentTimeMillis()

  /**
    * 初始化Fetcher域下相关组件
    */
  override protected def initFetchers: Unit = {
    log.info(s"fetcherManager start init fetchers,id:$syncTaskId")
    //todo 动态组件能力
    //    val directFetcherTypeName = taskManager.fetcherNameToLoad.get(directFetcherName).flatMap(Option(_)).getOrElse(SimpleOplogFetcher.name)
    //构建directFetcher
    log.info(s"start init $directFetcherName,id:$syncTaskId")
    context.actorOf(SimpleOplogFetcher.props(taskManager.asInstanceOf[MongoSourceManagerImp with TaskManager], batcher).withDispatcher("akka.pinned-dispatcher"), directFetcherName)
  }


  override def receive: Receive = {
    case SyncControllerMessage(OplogFetcherStart) => start
  }

  def online: Receive = {
    case SyncControllerMessage(OplogFetcherUpdateDelay(x)) => dispatchUpdateDelayMessage(x)
    case FetcherMessage(OplogFetcherCheckActive) => handleCheckActiveTask
    case OplogFetcherActiveChecked(ts) => lastActive = ts
    case FetcherMessage(OplogFetcherActiveChecked(ts)) => lastActive = ts
    case OplogFetcherFree => fetcherChangeStatus(Status.FREE)
    case OplogFetcherBusy => fetcherChangeStatus(Status.BUSY)
  }

  /**
    * 处理检查活跃性任务
    *
    */
  private def handleCheckActiveTask: Unit = {
    val now = System.currentTimeMillis()
    val diff = now - lastActive
    if (diff > 90 * 1000) throw new RuntimeException(s"$diff seconds passed which gt 90s ,but direct fetcher has no response,id:$syncTaskId")
    dispatchMessageToDirectFetcher(OplogFetcherCheckActive)
    context.system.scheduler.scheduleOnce(SettingConstant.CHECK_ACTIVE_INTERVAL second, self, OplogFetcherCheckActive)
  }

  private def dispatchUpdateDelayMessage(x:Long): Unit = dispatchMessageToDirectFetcher(FetcherMessage(OplogFetcherUpdateDelay(x)))

  private def dispatchMessageToDirectFetcher(m: Any): Unit = directFetcher.map(ref => ref ! m)

  private def start: Unit = {
    log.info(s"oplog fetcher manager start,id:$syncTaskId")
    fetcherChangeStatus(Status.ONLINE)
    context.children.foreach(ref => ref ! FetcherMessage(OplogFetcherStart))
    self ! FetcherMessage(OplogFetcherCheckActive) //检测活性
    context.become(online, true)
  }

  override def preStart(): Unit

  = {
    log.info(s"fetcherManger switch to offline,id:$syncTaskId")
    context.become(receive, true)
    initFetchers
    //状态置为offline
    fetcherChangeStatus(Status.OFFLINE)
  }

  override def postRestart(reason: Throwable): Unit

  = {
    log.info(s"fetcherManger processing postRestart,id:$syncTaskId")
    super.postRestart(reason)

  }

  override def postStop(): Unit

  = {
    log.info(s"fetcherManger processing postStop,id:$syncTaskId")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit

  = {
    log.info(s"fetcherManger processing preRestart,id:$syncTaskId")
    context.become(receive, true)
    fetcherChangeStatus(Status.RESTARTING)
    super.preRestart(reason, message)
    log.info(s"fetcherManger processing preRestart complete,id:$syncTaskId")
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
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???
}

object OplogFetcherManager {
  def props(taskManager: TaskManager, batcher: ActorRef): Props = Props(new OplogFetcherManager(taskManager, batcher))
}