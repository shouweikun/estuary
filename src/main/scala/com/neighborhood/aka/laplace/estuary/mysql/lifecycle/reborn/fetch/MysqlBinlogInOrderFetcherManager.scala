package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{ActorRef, AllForOneStrategy, Props}
import com.neighborhood.aka.laplace.estuary.bean.exception.control.WorkerCannotFindException
import com.neighborhood.aka.laplace.estuary.bean.exception.fetch._
import com.neighborhood.aka.laplace.estuary.bean.exception.schema.SchemaException
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataFetcherManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{FetcherMessage, SyncControllerMessage, WorkerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.adapt.MysqlBinlogInOrderPowerAdapterCommand.MysqlBinlogInOrderPowerAdapterDelayFetch
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch.MysqlBinlogInOrderFetcherCommand._
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp


/**
  * Created by john_liu on 2018/2/5.
  * 对应同步域中的Fetcher
  *
  * @note 本类的子Actor需求:
  *       $directFetcherName 默认加载Simple
  * @author neighborhood.aka.laplace
  */

final class MysqlBinlogInOrderFetcherManager(
                                              override val taskManager: MysqlSourceManagerImp with TaskManager,
                                              val binlogEventBatcher: ActorRef
                                            ) extends SourceDataFetcherManagerPrototype {
  val directFetcherName: String = "directFetcher"

  implicit private lazy val ec = context.dispatcher
  /**
    *
    * 任务id
    */
  override val syncTaskId = taskManager.syncTaskId

  /**
    * 是否是最上层的manager
    */
  override def isHead: Boolean = true

  /**
    * batcher 的ActorRef
    *
    */
  override val batcher: ActorRef = binlogEventBatcher

  /**
    * 数据拉取
    *
    * @return
    */
  def directFetcher: Option[ActorRef] = context.child(directFetcherName)

  /**
    * 快照判断
    *
    * @return
    */
  //  def snapshotJudger: Option[ActorRef] = context.child("snapshotJudger")

  /**
    * 重试机制
    */
  override val errorCountThreshold: Int = 3
  override var errorCount: Int = 0


  /**
    * OFFLINE/SUSPEND状态
    * 1.FetcherMessage(MysqlBinlogInOrderFetcherRestart) 重启任务
    * 2.SyncControllerMessage(MysqlBinlogInOrderFetcherRestart) 开始任务
    *   2.1 寻找到开始position并切换为开始模式
    * 3.FetcherMessage("resume") resume任务
    *
    * @return
    */
  override def receive: Receive = {

    case FetcherMessage(MysqlBinlogInOrderFetcherRestart) => restart
    case SyncControllerMessage(MysqlBinlogInOrderFetcherRestart) => restart
    case SyncControllerMessage(MysqlBinlogInOrderFetcherStart) => start
  }

  /**
    * 1. FetcherMessage("start") 执行onStart
    * 2. FetcherMessage("prefetch") 执行prefetch，为拉取数据初始化环境
    * 3. FetcherMessage("fetch") 阻塞方式拉取数据
    * 4. FetcherMessage("nonBlockingFetch") (1)非阻塞方式拉取数据(2)处理快照任务
    * 5. FetcherMessage("restart") 重新启动
    * 6. SyncControllerMessage("suspend") 挂起
    */
  def online: Receive = {
    case FetcherMessage(MysqlBinlogInOrderFetcherRestart) => restart
    case FetcherMessage(MysqlBinlogInOrderFetcherSuspend) => suspend
    case FetcherMessage(MysqlBinlogInOrderFetcherBusy) => fetcherChangeStatus(Status.BUSY)
    case FetcherMessage(MysqlBinlogInOrderFetcherFree) => fetcherChangeStatus(Status.FREE)
    case FetcherMessage(msg) => log.warning(s"fetcher online unhandled command:$msg,id:$syncTaskId")
    case SyncControllerMessage(MysqlBinlogInOrderFetcherSuspend) => suspend
    case SyncControllerMessage(MysqlBinlogInOrderPowerAdapterDelayFetch(d))  =>dispatchFetchDelay(d)

  }

  private def dispatchFetchDelay(fetchDelay: Long): Unit = {
    directFetcher.fold(throw new WorkerCannotFindException(s"cannot find worker:$directFetcherName when ,id:$syncTaskId"))(ref => ref ! FetcherMessage(MysqlBinlogInOrderFetcherUpdateDelay(fetchDelay)))
  }


  private def restart: Unit = {
    log.info(s"fetcher restarting,id:$syncTaskId")
    context.become(receive, true)
    self ! SyncControllerMessage(MysqlBinlogInOrderFetcherStart)
  }

  /**
    * 初始化
    * 如果快照任务是suspend状态，切换为suspend
    * 否则开始正常的同步任务初始化
    *
    */
  private def start: Unit = {
    context.children.foreach(ref => ref ! FetcherMessage(MysqlBinlogInOrderFetcherStart))
    fetcherChangeStatus(Status.ONLINE)
    context.become(online, true)
  }

  /**
    * 挂起操作
    *
    * 1.挂起子节点
    * 2.挂起自己进入offline
    * 3.状态置为suspend
    */
  private def suspend: Unit = {
    context.children.foreach(ref => ref ! FetcherMessage(MysqlBinlogInOrderFetcherSuspend))
    context.become(receive, true)
    fetcherChangeStatus(Status.SUSPEND)
  }

  /**
    * 初始化Fetcher域下相关组件
    * 1.初始化DirectFetcher
    */
  protected def initFetchers: Unit = {
    val directFetcherTypeName = taskManager.fetcherNameToLoad.get(directFetcherName).flatMap(Option(_)).getOrElse(SimpleMysqlBinlogInOrderDirectFetcher.name)


    //构建directFetcher
    log.info(s"start init $directFetcherName,id:$syncTaskId")
    context.actorOf(MysqlBinlogInOrderDirectFetcher.buildMysqlBinlogInOrderDirectFetcher(taskManager, batcher, directFetcherTypeName).withDispatcher("akka.pinned-dispatcher"), directFetcherName)
  }

  /**
    * ***************************错误处理**************************************
    */
  override def processError(e: Throwable, message: WorkerMessage): Unit

  = {
    e.printStackTrace()
    if (e.isInstanceOf[SchemaException]) errorCount = errorCountThreshold
    if (isCrashed) {
      fetcherChangeStatus(Status.ERROR)
      errorCount = 0
      throw new OutOfFetchRetryThersholdException({
        log.warning(s"fetcher throws exception $e,cause:${
          e.getCause
        },id:$syncTaskId");
        s"fetching data failure for 3 times,id:$syncTaskId"
      }, e
      )
    } else {
      self ! message
    }
  }

  /**
    * ********************* 状态变化 *******************
    */
  override def changeFunc(status: Status): Unit

  = TaskManager.changeFunc(status, taskManager)

  override def onChangeFunc: Unit

  = TaskManager.onChangeStatus(taskManager)

  override def fetcherChangeStatus(status: Status): Unit

  = TaskManager.changeStatus(status, changeFunc, onChangeFunc)

  /**
    * ********************* Actor生命周期 *******************
    */
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

  override def supervisorStrategy = {
    AllForOneStrategy() {
      case e: Exception => {
        fetcherChangeStatus(Status.ERROR)
        log.error(s"fetcherManager crashed,exception:$e,cause:${e.getCause},processing SupervisorStrategy,id:$syncTaskId")
        Escalate
      }
      case error: Error => {
        fetcherChangeStatus(Status.ERROR)
        log.error(s"fetcherManager crashed,error:$error,cause:${error.getCause},processing SupervisorStrategy,id:$syncTaskId")
        Escalate
      }
      case e => {
        log.error(s"fetcherManager crashed,throwable:$e,cause:${e.getCause},processing SupervisorStrategy,id:$syncTaskId")
        fetcherChangeStatus(Status.ERROR)
        Escalate
      }
    }
  }
}

object MysqlBinlogInOrderFetcherManager {
  val name:String = MysqlBinlogInOrderFetcherManager.getClass.getName.stripSuffix("$")
  def props(taskManager: MysqlSourceManagerImp with TaskManager, binlogEventBatcher: ActorRef): Props = Props(new MysqlBinlogInOrderFetcherManager(taskManager, binlogEventBatcher))
}




