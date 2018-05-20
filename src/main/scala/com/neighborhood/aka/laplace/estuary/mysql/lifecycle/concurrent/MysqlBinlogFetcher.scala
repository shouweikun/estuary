package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.concurrent

import java.util.concurrent.Executors

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props}
import com.alibaba.otter.canal.parse.exception.TableIdNotFoundException
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.{SourceDataFetcher, Status}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{FetcherMessage, SyncControllerMessage, WorkerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.utils.{CanalEntryJsonHelper, MysqlBinlogParser}
import org.I0Itec.zkclient.exception.ZkTimeoutException
import org.apache.commons.lang.StringUtils

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Created by john_liu on 2018/2/5.
  */

class MysqlBinlogFetcher(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager, binlogEventBatcher: ActorRef, binlogDdlHandler: ActorRef = null) extends Actor with SourceDataFetcher with ActorLogging {

  implicit val transTaskPool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  /**
    * binlogParser 解析binlog
    */
  lazy val binlogParser: MysqlBinlogParser = mysql2KafkaTaskInfoManager.binlogParser

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


  //offline
  override def receive: Receive = {

    case FetcherMessage(msg) => {
      msg match {
        case "restart" => {
          log.info("fetcher restarting")
          self ! SyncControllerMessage("start")
        }
        case str: String => {
          log.warning(s"fetcher offline  unhandled message:$str")
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
              log.error(s"fetcher find entryPosition is null,id:$syncTaskId")
              throw new Exception(s"entryPosition is null when find position,id:$syncTaskId")
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

          mysqlConnection.map {
            conn =>
              log.debug("fetcher predump")
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
            val fetchDelay = mysql2KafkaTaskInfoManager.taskInfo.fetchDelay.get
            context.system.scheduler.scheduleOnce(fetchDelay microseconds, self, FetcherMessage("fetch"))
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
    case SyncControllerMessage(msg) => {

    }
  }

  @deprecated("use `fetchOne`")
  @tailrec
  final def loopFetchAll: Unit = {
    fetchOne()
    loopFetchAll
  }

  /**
    * 从连接中取数据
    */
  def fetchOne(before: Long = System.currentTimeMillis()) = {
    lazy val entry = mysqlConnection.get.fetchUntilDefined(filterEntry(_))(binlogParser)
    if (entry.get.getHeader.getEventType == CanalEntry.EventType.ALTER) {
      log.info(s"fetch ddl:${CanalEntryJsonHelper.entryToJson(entry.get)},id:$syncTaskId");
      Option(binlogDdlHandler).fold(log.warning(s"ddlHandler does not exist,id:$syncTaskId"))(x => x ! entry.get)
    } else binlogEventBatcher ! entry.get
    lazy val cost = System.currentTimeMillis() - before
    if (isCounting) mysql2KafkaTaskInfoManager.fetchCount.incrementAndGet()
    if (isCosting) mysql2KafkaTaskInfoManager.powerAdapter.fold(log.warning(s"powerAdapter not exist,id:$syncTaskId"))(x => x ! FetcherMessage(s"$cost"))

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
    log.warning(s"fetcher throws exception $e,cause:${e.getCause},id:$syncTaskId")
    errorCount += 1
    if (isCrashed) {
      fetcherChangeStatus(Status.ERROR)
      errorCount = 0
      println(message.msg)
      e.printStackTrace()
      throw new Exception(s"fetching data failure for 3 times,id:$syncTaskId",e)
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
    if (mysqlConnection.isDefined && mysqlConnection.get.isConnected) mysqlConnection.get.disconnect()
    //状态置为offline
    fetcherChangeStatus(Status.OFFLINE)
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)

    log.info("fetcher will restart in 1 min")
  }

  override def postStop(): Unit = {
    mysqlConnection.get.disconnect()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {

    context.become(receive)
    fetcherChangeStatus(Status.RESTARTING)
    super.preRestart(reason, message)
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: ZkTimeoutException => {
        fetcherChangeStatus(Status.ERROR)
        Restart
      }
      case e: Exception => {
        fetcherChangeStatus(Status.ERROR)
        Restart
      }
      case error: Error => {
        fetcherChangeStatus(Status.ERROR)
        Restart
      }
      case _ => {
        fetcherChangeStatus(Status.ERROR)
        Restart
      }
    }
  }

}

object MysqlBinlogFetcher {
  def props(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager, binlogEventBatcher: ActorRef, binlogDdlHandler: ActorRef = null): Props = {
    Props(new MysqlBinlogFetcher(mysql2KafkaTaskInfoManager, binlogEventBatcher, binlogDdlHandler))
  }
}
