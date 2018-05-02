package com.neighborhood.aka.laplace.estuary.mysql.lifecycle

import java.util.concurrent.Executors

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props}
import com.alibaba.otter.canal.parse.exception.{CanalParseException, TableIdNotFoundException}
import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.DirectLogFetcher
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplace.estuary.core.lifecycle.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SourceDataFetcher, Status, _}
import com.neighborhood.aka.laplace.estuary.core.source.MysqlConnection
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.{CanalEntryJsonHelper, Mysql2KafkaTaskInfoManager, MysqlBinlogParser}
import com.taobao.tddl.dbsync.binlog.{LogContext, LogDecoder}
import org.I0Itec.zkclient.exception.ZkTimeoutException
import org.apache.commons.lang.StringUtils

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

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
              log.error("fetcher find entryPosition is null")
              throw new Exception("entryPosition is null when find position")
            } {
              thePosition =>
                mysqlConnection.map(_.connect())
                log.info(s"fetcher find start position,binlogFileName:${thePosition.getJournalName},${thePosition.getPosition}")
                context.become(online)
                log.info(s"fetcher switch to online")
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
              log.error("unsupported operation: dump by timestamp is not supported yet")
              throw new UnsupportedOperationException("unsupported operation: dump by timestamp is not supported yet")
            } else {
              log.info("start dump binlog")
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
          println(s"fetcher online unhandled command:$str")
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
      log.info(s"fetch ddl:${CanalEntryJsonHelper.entryToJson(entry.get)}");
      Option(binlogDdlHandler).fold(log.warning("ddlHandler does not exist"))(x => x ! entry.get)
    } else binlogEventBatcher ! entry.get
    lazy val cost = System.currentTimeMillis() - before
    if (isCounting) mysql2KafkaTaskInfoManager.fetchCount.incrementAndGet()
    if (isCosting) mysql2KafkaTaskInfoManager.powerAdapter.fold(log.warning("powerAdapter not exist"))(x => x ! FetcherMessage(s"$cost"))

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
    log.warning(s"fetcher throws exception $e")
    errorCount += 1
    if (isCrashed) {
      fetcherChangeStatus(Status.ERROR)
      errorCount = 0
      println(message.msg)
      e.printStackTrace()
      throw new Exception("fetching data failure for 3 times")
    } else {
      self ! message
    }
  }

  /**
    * ********************* 状态变化 *******************
    */
  private def changeFunc(status: Status) = TaskManager.changeFunc(status, mysql2KafkaTaskInfoManager)

  private def onChangeFunc = Mysql2KafkaTaskInfoManager.onChangeStatus(mysql2KafkaTaskInfoManager)

  private def fetcherChangeStatus(status: Status) = TaskManager.changeStatus(status, changeFunc, onChangeFunc)

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
