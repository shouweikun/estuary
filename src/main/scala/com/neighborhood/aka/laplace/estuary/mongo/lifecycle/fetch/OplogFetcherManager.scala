package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{FetcherMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataFetcherManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoSourceManagerImp
import OplogFetcherCommand._
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status

/**
  * Created by john_liu on 2019/3/3.
  *
  * @author neighborhood.aka.laplace
  * @note
  */
final class OplogFetcherManager(override val taskManager: TaskManager,
                                override val batcher: ActorRef) extends SourceDataFetcherManagerPrototype {
  /**
    * 是否是最上层的manager
    */
  override def isHead: Boolean = true

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

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
    case m@SyncControllerMessage(OplogFetcherUpdateDelay(_)) => dispatchFetchDelayMessage(m)
  }

  private def dispatchFetchDelayMessage(m: Any): Unit = directFetcher.map(ref => ref ! m)

  private def start: Unit = {
    log.info(s"oplog fetcher manager start,id:$syncTaskId")
    fetcherChangeStatus(Status.ONLINE)
    context.children.foreach(ref => ref ! FetcherMessage(OplogFetcherStart))
    context.become(online, true)
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
