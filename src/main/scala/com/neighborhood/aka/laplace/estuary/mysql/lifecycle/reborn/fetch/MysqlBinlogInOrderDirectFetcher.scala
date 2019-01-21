package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

import java.io.IOException

import akka.actor.{ActorRef, Props}
import com.alibaba.otter.canal.parse.exception.TableIdNotFoundException
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplace.estuary.bean.exception.fetch._
import com.neighborhood.aka.laplace.estuary.bean.exception.schema.SchemaException
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.DataSourceFetcherPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{FetcherMessage, WorkerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.{SourceManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.adapt.MysqlBinlogInOrderPowerAdapterCommand.MysqlBinlogInOrderPowerAdapterUpdateCost
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch.MysqlBinlogInOrderFetcherCommand._
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp
import com.neighborhood.aka.laplace.estuary.mysql.source.{MysqlConnection, MysqlSourceManagerImp}
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2MysqlTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.utils.CanalEntryTransUtil

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.Try

/**
  * Created by john_liu on 2018/2/5.
  * 对应同步域中的Fetcher
  *
  * @todo 重新设计快照
  * @todo 非阻塞拉取
  * @author neighborhood.aka.laplace
  * @note 增加一个新的directFetcher時，在工廠方法中增加
  */

abstract class MysqlBinlogInOrderDirectFetcher(
                                                override val taskManager: MysqlSourceManagerImp with TaskManager,
                                                val downStream: ActorRef
                                              ) extends DataSourceFetcherPrototype[MysqlConnection] {
  val specialCounterName: String = "counter" //special Counter名称
  implicit val transTaskPool: ExecutionContextExecutor = scala.concurrent.ExecutionContext.Implicits.global //使用广域线程池

  /**
    * 资源管理器
    */
  override val sourceManager: SourceManager[MysqlConnection] = taskManager
  /**
    * 功率调节器
    */
  lazy val powerAdapter = taskManager.powerAdapter
  /**
    * fetcher专用counter
    */
  lazy val fetcherCounter = context.child(specialCounterName)
  /**
    * 任务id
    */
  val syncTaskId = taskManager.syncTaskId

  /**
    * 重试机制
    */
  override def errorCountThreshold: Int = 3

  override var errorCount: Int = 0
  /**
    * Schema模块是否开启
    */
  val schemaComponentIsOn = taskManager.schemaComponentIsOn
  /**
    * 是否记录耗时
    */
  val isCosting = taskManager.isCosting
  /**
    * 是否计数
    */
  val isCounting = taskManager.isCounting
  /**
    * 打包閾值
    */
  val batchThreshold = taskManager.batchThreshold
  /**
    * 是否是batch发送
    */
  val isBatchSend = false //todo 一批一批发送
  /**
    * 是否是阻塞式拉取
    */
  val isBlockingFetch = true //taskManager.isBlockingFetch 暂时不支持非阻塞拉取
  /**
    * 是否执行ddl
    */
  val isNeedExecuteDDl: Boolean = taskManager.isNeedExecuteDDL
  /** 感觉
    * 拉取数据上下文初始化管理器
    */
  val fetchContextInitializer = taskManager.fetchContextInitializer
  /**
    * 數據拉取管理器
    */
  val fetchEntryHandler = taskManager.fetchEntryHandler
  /**
    * mysql链接
    * 必须是fork 出来的
    */
  val mysqlConnection: Option[MysqlConnection] = Option(connection.asInstanceOf[MysqlConnection])
  /**
    * 暂存的entryPosition
    */
  var entryPosition: Option[EntryPosition] = None
  /**
    * fetchDelay
    */
  var fetchDelay: Long = 0l

  /**
    * OFFLINE/SUSPEND状态
    * 1.FetcherMessage(MysqlBinlogInOrderFetcherReStart) 重启任务
    * 2.FetcherMessage(MysqlBinlogInOrderResume) resume任务
    * 3.FetcherMessage(MysqlBinlogInOrderFetcherStart) 开始任务
    *  3.1 寻找到开始position并切换为开始模式
    * 4.FetcherMessage(MysqlBinlogInOrderPrefetch) 执行prefetch，为拉取数据初始化环境
    * 5.FetcherMessage(MysqlBinlogInOrderFetch) 阻塞方式拉取数据
    * 6.FetcherMessage(MysqlBinlogInOrderFetcherRestart) 重新启动
    * 7.FetcherMessage("suspend") 挂起
    * 8.FetcherMessage((entry: CanalEntry.Entry, cost: Long)) 处理数据
    *
    * @return
    */
  override def receive: Receive = {
    case FetcherMessage(MysqlBinlogInOrderFetcherFetch) => Try(handleFetchTask(true))
      .failed
      .foreach(processError(_, FetcherMessage(MysqlBinlogInOrderFetcherFetch)))
    case FetcherMessage(MysqlBinlogInOrderFetcherUpdateDelay(x)) => fetchDelay = x //更新fetchDelay
    case FetcherMessage(MysqlBinlogInOrderFetcherRestart) => restart
    case FetcherMessage(MysqlBinlogInOrderFetcherStart) => Try(
      start(mysqlConnection))
      .failed
      .foreach(e => processError(e, FetcherMessage(MysqlBinlogInOrderFetcherStart)))
    case FetcherMessage(MysqlBinlogInOrderFetcherPrefetch) => Try(preFetch())
      .failed
      .foreach(e => processError(e, FetcherMessage(MysqlBinlogInOrderFetcherPrefetch)))

  }

  /**
    * 挂起状态
    */
  def suspendStatus: Receive = {
    case FetcherMessage(MysqlBinlogInOrderResume) => resume
    case _ =>
  }

  /**
    * 重启任务
    * 1.切换成suspend
    * 2.然后在开始
    */
  private def restart: Unit = {
    log.info(s"fetcher restarting,id:$syncTaskId");
    suspend(mysqlConnection)
    resume
    self ! FetcherMessage(MysqlBinlogInOrderFetcherStart)
  }

  /**
    * 初始化
    * 如果快照任务是suspend状态，切换为suspend
    * 否则开始正常的同步任务初始化
    *
    * @param mysqlConnection
    */
  private def start(mysqlConnection: => Option[MysqlConnection] = this.mysqlConnection): Unit = {
    log.info(s"MysqlBinlogInOrderDirectFetcher process start,id:$syncTaskId")
    entryPosition = findPositionAndSwitch2Start(mysqlConnection)
    self ! FetcherMessage(MysqlBinlogInOrderFetcherPrefetch)
  }

  /**
    * 切换为suspend
    *
    * @param mysqlConnection
    */
  private def suspend(mysqlConnection: Option[MysqlConnection]): Unit = {
    log.info(s"fetcher switched to suspend,id:$syncTaskId")
    mysqlConnection.map(_.disconnect()) //断开数据库链接
    entryPosition = None
    context.become(suspendStatus, true)
  }

  /**
    * 切换为resume
    */
  private def resume: Unit = {
    log.info(s"fetcher resuming,id:$syncTaskId")
    context.become(receive, true)
    self ! FetcherMessage("start")
  }


  /**
    * preFetch阶段
    */
  private def preFetch(
                        mysqlConnection: Option[MysqlConnection] = this.mysqlConnection,
                        entryPosition: Option[EntryPosition] = this.entryPosition
                      ): Unit = {
    log.info(s"directFetcher processing preFetch,id:$syncTaskId")
    fetchContextInitializer.preFetch(mysqlConnection)(entryPosition)
    lazy val message: MysqlBinlogInOrderFetcherCommand = isBlockingFetch match {
      case true => MysqlBinlogInOrderFetcherFetch
      case false => MysqlBinlogInOrderFetcherNonBlockingFetch
    }
    self ! FetcherMessage(message)
  }


  /**
    * 处理拉取数据任务
    * 1.获取打包大小(默认1)
    * 2.拉取数据
    * 3.发送数据
    *  3.1 发送数据
    *  3.2 计算数量
    *  3.3 计算耗时
    * 4.更新delay
    * 5.处理拉取数据任务
    */
  private def handleFetchTask(isBlocking: Boolean = true): Unit = {
    def blockingHandle: Unit = {
      val theBatchThreshold = batchThreshold
      val entriesAndCosts = fetchEntryHandler
        .blockingHandleFetchTask(theBatchThreshold)(mysqlConnection)
      if (isBatchSend) sendDataAndHandleOtherTaskIfNecessaryWithBatch(entriesAndCosts) else entriesAndCosts.map { case (entry, cost) => sendDataAndHandleOtherTaskIfNecessary(entry, cost) }

    }

    def nonBlockingHandle: Unit = ??? //暂时不支持非阻塞


    //轉為fetching
    fetchEntryHandler.switchToFetching
    lazy val fetchMessage = if (isBlocking) MysqlBinlogInOrderFetcherFetch else MysqlBinlogInOrderFetcherNonBlockingFetch
    //發送下次的拉取命令
    sendFetchMessage(self, fetchDelay, fetchMessage)
    //告訴上級處於拉取狀態
    context.parent ! FetcherMessage(MysqlBinlogInOrderFetcherBusy)
    //判断是阻塞方式还是非阻塞方式
    if (isBlocking) blockingHandle else nonBlockingHandle
    //刷新为free
    context.parent ! FetcherMessage(MysqlBinlogInOrderFetcherFree)
  }

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
    * 发送批量数据
    *
    * @param list
    */
  private def sendDataAndHandleOtherTaskIfNecessaryWithBatch(list: List[(CanalEntry.Entry, Long)]): Unit = {
    val (entries, costs) = list.unzip
    sendEntries(entries)
    sendCounts(entries)
    sendCosts(costs)
    //如果处理了ddl，那么下次数据拉取+10000微秒
    //    if (CanalEntryTransUtil.isDdl(entry)) fetchDelay += 10000


  }

  /**
    * 发送数据并处理一些必要任务
    *
    * @param entry
    * @param cost
    */
  private def sendDataAndHandleOtherTaskIfNecessary(entry: CanalEntry.Entry, cost: Long): Unit = {
    sendDataTask(entry, cost)
    //如果处理了ddl，那么下次数据拉取+10000微秒
    //    if (CanalEntryTransUtil.isDdl(entry)) fetchDelay += 10000
    fetchEntryHandler.switchToFree


  }

  protected def executeDdl(entry: CanalEntry.Entry): Unit

  /**
    * 处理数据发送任务
    * 1.向batcher发送entry
    * 2.向fetcherCounter发送entry用于计数
    * 3.向powerAdapter发送cost用于计算耗时
    *
    * @param entry
    * @param cost
    */
  private def sendDataTask(entry: => CanalEntry.Entry, cost: Long): Unit = {
    if (CanalEntryTransUtil.isDdl(entry)) {
      if (isNeedExecuteDDl) executeDdl(entry)
    }
    else {
      sendEntry(entry)
      if (CanalEntryTransUtil.isDml(entry)) {
        sendCount(entry)
        sendCost(cost)
      }
    }
  }

  /**
    * 發送entry給Batcher
    *
    * @param entry
    */
  private def sendEntry(entry: CanalEntry.Entry) = downStream ! entry

  /**
    * 发送一批entries
    *
    * @param entrys
    */
  private def sendEntries(entrys: List[CanalEntry.Entry]) = downStream ! entrys

  /**
    * 發送計數
    *
    * @param entry
    */
  private def sendCount(entry: CanalEntry.Entry) = if (isCounting) fetcherCounter.fold(log.warning(s"fetcherCounter not exist,id:$syncTaskId"))(ref => ref ! entry)

  private def sendCounts(entrys: List[CanalEntry.Entry]) = if (isCounting) fetcherCounter.fold(log.warning(s"fetcherCounter not exist,id:$syncTaskId"))(ref => ref ! entrys)

  /**
    * 發送耗時
    *
    * @param theCost
    */
  private def sendCost(theCost: Long) = if (isCosting) powerAdapter.fold(log.warning(s"powerAdapter not exist,id:$syncTaskId"))(x => x ! FetcherMessage(MysqlBinlogInOrderPowerAdapterUpdateCost(theCost)))

  private def sendCosts(theCosts: List[Long]) = if (isCosting) powerAdapter.fold(log.warning(s"powerAdapter not exist,id:$syncTaskId"))(x => x ! FetcherMessage(MysqlBinlogInOrderPowerAdapterUpdateCost(theCosts.sum / theCosts.size)))

  /**
    * 开始前准备
    * 1.链接mysql
    * 2.初始化Schema信息相关操作
    * 3.存一下开始位置,避免启动失败导致出现binlog未能记录的问题
    * 4.切换为online模式
    * 5.发起开始命令
    *
    * @param mysqlConnection
    */
  private def findPositionAndSwitch2Start(mysqlConnection: Option[MysqlConnection]): Option[EntryPosition] = {
    val entryPosition = mysqlConnection.map(fetchContextInitializer.findStartPosition(_))
    log.info(s"MysqlBinlogInOrderDirectFetcher find start position finished,id:$syncTaskId")
    entryPosition.fold(
      throw new EmptyEntryException(s"entryPosition is null when switch2Start ,$syncTaskId")
    )(fetchContextInitializer.onPrepareStart(mysqlConnection, _))
    log.info(s"directFetcher switch to online,id:$syncTaskId")
    entryPosition
  }

  /**
    * 处理TableIdNotFoundException
    *
    * @param message
    */
  private def handleTableIdNotFoundException(message: WorkerMessage = FetcherMessage("start")) = {
    entryPosition = fetchContextInitializer.findStartPositionWithTransaction(mysqlConnection);
    self ! message
  }

  //以下是业务无关方法

  /**
    * ***************************错误处理**************************************
    */
  override def processError(e: Throwable, message: WorkerMessage): Unit

  = {

    def handleNormalException = if (isCrashed) {
      errorCount = 0
      throw new OutOfFetchRetryThersholdException({
        log.warning(s"fetcher throws exception $e,cause:${
          e.getCause
        },id:$syncTaskId");
        s"fetching data failure for 3 times,id:$syncTaskId"
      }, e
      )
    } else {
      errorCount += 1
      self ! message
    }

    log.warning(s"MysqlBinlogInOrderDirectFetcher process processError,message:$message,id:$syncTaskId")
    e.printStackTrace()
    e match {
      case ex: java.nio.channels.ClosedChannelException =>
      case ex: SchemaException => throw e
      case ex: IOException => throw e
      case ex: TableIdNotFoundException => handleTableIdNotFoundException(message)
      case ex => handleNormalException
    }


  }

  /**
    * ********************* Actor生命周期 *******************
    */
  override def preStart(): Unit

  = {
    log.info(s"fetcher switch to offline,id:$syncTaskId")
    context.become(receive, true)
    mysqlConnection.map(conn => if (conn.isConnected) conn.disconnect())
    context.actorOf(MysqlBinlogInOrderFetcherCounter.props(taskManager).withDispatcher(("akka.fetcher-dispatcher")), specialCounterName)

    //状态置为offline

  }

  override def postRestart(reason: Throwable): Unit

  = {
    log.info(s"directFetcher processing postRestart,id:$syncTaskId")
    super.postRestart(reason)

  }

  override def postStop(): Unit

  = {
    log.info(s"directFetcher processing postStop,id:$syncTaskId")
    mysqlConnection.map(conn => if (conn.isConnected) conn.disconnect())
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit

  = {
    log.info(s"directFetcher processing preRestart,id:$syncTaskId")
    context.become(receive, true)

    super.preRestart(reason, message)
  }


}

object MysqlBinlogInOrderDirectFetcher {
  /**
    * 構建DirectFetcher的工廠方法
    *
    * @param taskManager 傳入taskManager
    * @param downStream  下游
    * @param name        名稱
    * @return 構建好的props
    */
  def buildMysqlBinlogInOrderDirectFetcher(taskManager: MysqlSourceManagerImp with TaskManager,
                                           downStream: ActorRef, name: String): Props = name match {
    case SimpleMysqlBinlogInOrderDirectFetcher.name => SimpleMysqlBinlogInOrderDirectFetcher.props(taskManager.asInstanceOf[MysqlSinkManagerImp with MysqlSourceManagerImp with TaskManager], downStream)
    case SdaMysqlBinlogInOrderDirectFetcher.name => SdaMysqlBinlogInOrderDirectFetcher.props(taskManager.asInstanceOf[Mysql2MysqlTaskInfoManager], downStream)
    case DefaultMysqlBinlogInOrderDirectFetcher.name => DefaultMysqlBinlogInOrderDirectFetcher.props(taskManager, downStream)
    case _ => DefaultMysqlBinlogInOrderDirectFetcher.props(taskManager, downStream)
  }

}





