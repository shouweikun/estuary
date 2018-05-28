package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.alibaba.otter.canal.parse.exception.TableIdNotFoundException
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplace.estuary.bean.exception.fetch.{CannotFindOffsetException, FetchDataException, NullOfDataSourceConnectionException, OutOfFetchRetryThersholdException}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.{SourceDataFetcher, Status}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{FetcherMessage, SyncControllerMessage, WorkerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.utils.MysqlBinlogParser
import org.apache.commons.lang.StringUtils

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

/**
  * Created by john_liu on 2018/2/5.
  */

class MysqlBinlogInOrderFetcher(
                                 val mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,
                                 val binlogEventBatcher: ActorRef
                               ) extends Actor with SourceDataFetcher with ActorLogging {

  implicit val transTaskPool: ExecutionContextExecutor = context.dispatcher
  /**
    * binlogParser 解析binlog
    */
  lazy val binlogParser: MysqlBinlogParser = mysql2KafkaTaskInfoManager.binlogParser
  /**
    * 功率调节器
    */
  lazy val powerAdapter = mysql2KafkaTaskInfoManager.powerAdapter
  /**
    * fetcher专用counter
    */
  lazy val fetcherCounter = context.child("counter")
  /**
    * 任务id
    */
  val syncTaskId = mysql2KafkaTaskInfoManager.syncTaskId
  /**
    * 重试机制
    */
  override var errorCountThreshold: Int = 3
  override var errorCount: Int = 0
  /**
    * 模拟的从库id
    */
  val slaveId = mysql2KafkaTaskInfoManager.slaveId
  /**
    * mysql链接
    * 必须是fork 出来的
    */
  val mysqlConnection: Option[MysqlConnection] = Option(mysql2KafkaTaskInfoManager.mysqlConnection.fork)
  /**
    * 寻址处理器
    */
  val logPositionHandler = mysql2KafkaTaskInfoManager.logPositionHandler
  /**
    * 是否记录耗时
    */
  val isCosting = mysql2KafkaTaskInfoManager.taskInfo.isCosting
  /**
    * 是否计数
    */
  val isCounting = mysql2KafkaTaskInfoManager.taskInfo.isCounting
  /**
    * 暂存的entryPosition
    */
  var entryPosition: Option[EntryPosition] = None
  var fetchDelay: Long = mysql2KafkaTaskInfoManager.fetchDelay.get()
  var count = 0

  //offline
  override def receive: Receive = {

    case FetcherMessage(msg) => {
      msg match {
        case "restart" => {
          log.info(s"fetcher restarting,id:$syncTaskId")
          self ! SyncControllerMessage("start")
        }
        case str: String => {
          log.warning(s"fetcher offline  unhandled message:$str,id:$syncTaskId")
        }
      }

    }
    case SyncControllerMessage(msg) => {
      msg match {
        case "stop" => {
          context.become(receive)
          fetcherChangeStatus(Status.OFFLINE)
        }
        case "start" => {
          try {
            entryPosition = mysqlConnection.map(_.fork).map { conn => conn.connect(); logPositionHandler.findStartPosition(conn) }
            entryPosition.fold {
              throw new CannotFindOffsetException(
                {
                  log.error(s"fetcher find entryPosition is null,id:$syncTaskId");
                  s"entryPosition is null when find position,id:$syncTaskId"
                }
              )
            } {
              thePosition =>
                mysqlConnection.map(_.connect())
                log.info(s"fetcher find start position,binlogFileName:${thePosition.getJournalName},${thePosition.getPosition},id:$syncTaskId")
                context.become(online)
                log.info(s"fetcher switch to online,id:$syncTaskId")
                self ! FetcherMessage("start")
            }
          }
          catch {
            case e: Exception => processError(e, SyncControllerMessage("start"))
          }

        }
      }
    }
  }

  def online: Receive = {
    case FetcherMessage(msg) => {
      msg match {
        case "start" => {
          fetcherChangeStatus(Status.ONLINE)
          self ! FetcherMessage("predump")
        }
        case "predump" => {

          mysqlConnection.fold {
            throw new NullOfDataSourceConnectionException({
              log.error(s"fetcher mysqlConnection cannot be null,id:$syncTaskId");
              s"fetcher mysqlConnection cannot be null,id:$syncTaskId"
            })
          } {
            conn =>
              log.debug(s"fetcher predump,id:$syncTaskId")
              MysqlConnection.preDump(conn)(binlogParser);
              conn.connect()
          }
          val startPosition = entryPosition.get
          try {
            if (StringUtils.isEmpty(startPosition.getJournalName) && Option(startPosition.getTimestamp).isEmpty) {
              log.error(s"unsupported operation: dump by timestamp is not supported yet,id:$syncTaskId")
              throw new UnsupportedOperationException(s"unsupported operation: dump by timestamp is not supported yet,id:$syncTaskId")
            } else {
              log.info(s"start dump binlog,id:$syncTaskId")
              MysqlConnection.dump(startPosition.getJournalName, startPosition.getPosition)(mysqlConnection.get)

            }
            self ! FetcherMessage("fetch")
          } catch {
            case e: Exception => processError(e, FetcherMessage("start"))
          }
        }
        case "fetch" => {
          val before = System.currentTimeMillis()
          try {
            fetchOne(before)
            val theFetchDelay = this.fetchDelay
            context.system.scheduler.scheduleOnce(theFetchDelay microseconds, self, FetcherMessage("fetch"))
          } catch {
            case e: TableIdNotFoundException => {
              entryPosition = Option(logPositionHandler.findStartPositionWithinTransaction(mysqlConnection.get))
              self ! FetcherMessage("start")
            }
            case e: Exception => processError(e, FetcherMessage("fetch"))
          }
        }
        case "restart" => {
          context.become(receive)
          self ! FetcherMessage("restart")
        }
        case str: String => {
          println(s"fetcher online unhandled command:$str,id:$syncTaskId")
        }
      }
    }
    case SyncControllerMessage(x: Long) => fetchDelay = x
    case SyncControllerMessage(x: Int) => fetchDelay = x
  }


  /**
    * 从连接中取数据
    */
  def fetchOne(before: Long = System.currentTimeMillis()) = {
    val entry = mysqlConnection.get.fetchUntilDefined(filterEntry(_))(binlogParser).get

    if (isCounting) fetcherCounter.fold(log.warning(s"fetcherCounter not exist,id:$syncTaskId"))(ref => ref ! entry)
    binlogEventBatcher ! entry
    //    println(entry.get.getEntryType)
    //    count = count + 1
    lazy val cost = System.currentTimeMillis() - before

    if (isCosting) powerAdapter.fold(log.warning(s"powerAdapter not exist,id:$syncTaskId"))(x => x ! FetcherMessage(cost))

  }

  /**
    * @param entryOption 得到的entry
    *                    对entry在类型级别上进行过滤
    */
  def filterEntry(entryOption: Option[CanalEntry.Entry]): Boolean = {
    if (!entryOption.isDefined) {
      return false
    }
    val entry = entryOption.get
    //我们只要rowdata和Transactioned
    if ((entry.getEntryType == CanalEntry.EntryType.ROWDATA) || (entry.getEntryType == CanalEntry.EntryType.TRANSACTIONEND)) true else {
      false
    }
  }

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: WorkerMessage): Unit = {

    errorCount += 1
    if (isCrashed) {
      fetcherChangeStatus(Status.ERROR)
      errorCount = 0
      throw new OutOfFetchRetryThersholdException(
        {
          log.warning(s"fetcher throws exception $e,cause:${e.getCause},id:$syncTaskId");
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
  private def changeFunc(status: Status): Unit = TaskManager.changeFunc(status, mysql2KafkaTaskInfoManager)

  private def onChangeFunc: Unit = TaskManager.onChangeStatus(mysql2KafkaTaskInfoManager)

  private def fetcherChangeStatus(status: Status): Unit = TaskManager.changeStatus(status, changeFunc, onChangeFunc)

  /**
    * ********************* Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    log.info(s"fetcher switch to offline,id:$syncTaskId")
    if (mysqlConnection.isDefined && mysqlConnection.get.isConnected) mysqlConnection.get.disconnect()
    context.actorOf(MysqlBinlogInOrderFetcherCounter.props(mysql2KafkaTaskInfoManager), "counter")
    //状态置为offline
    fetcherChangeStatus(Status.OFFLINE)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"fetcher processing postRestart,id:$syncTaskId")
    super.postRestart(reason)

  }

  override def postStop(): Unit = {
    log.info(s"fetcher processing postStop,id:$syncTaskId")
    mysqlConnection.get.disconnect()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"fetcher processing preRestart,id:$syncTaskId")
    context.become(receive)
    fetcherChangeStatus(Status.RESTARTING)
    super.preRestart(reason, message)
  }


}

object MysqlBinlogInOrderFetcher {
  def props(
             mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,
             binlogEventBatcher: ActorRef
           ): Props = Props(new MysqlBinlogInOrderFetcher(mysql2KafkaTaskInfoManager, binlogEventBatcher))
}

