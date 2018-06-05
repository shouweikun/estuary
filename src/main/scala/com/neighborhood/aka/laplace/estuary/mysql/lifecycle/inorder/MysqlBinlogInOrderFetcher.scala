package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.{ActorRef, Props}
import com.alibaba.otter.canal.parse.exception.TableIdNotFoundException
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplace.estuary.bean.exception.fetch._
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.DataSourceFetcherPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{FetcherMessage, SyncControllerMessage, WorkerMessage}
import com.neighborhood.aka.laplace.estuary.core.schema.HBaseEventualSinkSchemaHandler.HBaseTableInfo
import com.neighborhood.aka.laplace.estuary.core.task.{RecourceManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.utils.MysqlBinlogParser
import org.apache.commons.lang.StringUtils

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Created by john_liu on 2018/2/5.
  */

class MysqlBinlogInOrderFetcher(
                                 override val taskManager: Mysql2KafkaTaskInfoManager,
                                 val binlogEventBatcher: ActorRef
                               ) extends DataSourceFetcherPrototype[MysqlConnection] {

  implicit val transTaskPool: ExecutionContextExecutor = context.dispatcher
  /**
    * 资源管理器
    */
  override val recourceManager: RecourceManager[_, MysqlConnection, _] = taskManager
  /**
    * binlogParser 解析binlog
    */
  lazy val binlogParser: MysqlBinlogParser = taskManager.binlogParser
  /**
    * 功率调节器
    */
  lazy val powerAdapter = taskManager.powerAdapter
  /**
    * fetcher专用counter
    */
  lazy val fetcherCounter = context.child("counter")
  /**
    * schema信息处理
    */
  lazy val mysqlSchemaHandler = taskManager.mysqlSchemaHandler
  /**
    * 任务id
    */
  override val syncTaskId = taskManager.syncTaskId

  /**
    * 重试机制
    */
  override var errorCountThreshold: Int = 3
  override var errorCount: Int = 0
  /**
    * 模拟的从库id
    */
  val slaveId = taskManager.slaveId
  /**
    * mysql链接
    * 必须是fork 出来的
    */
  val mysqlConnection: Option[MysqlConnection] = Option(connection.asInstanceOf[MysqlConnection])
  /**
    * 寻址处理器
    */
  val logPositionHandler = taskManager.logPositionHandler
  /**
    * 是否记录耗时
    */
  val isCosting = taskManager.taskInfo.isCosting
  /**
    * 是否计数
    */
  val isCounting = taskManager.taskInfo.isCounting
  /**
    * 关注的库
    */
  val concernedDatabaseList = taskManager.taskInfo.concernedDatabase.split(",")
  /**
    * 是否初始化了
    */
  val isInitialized = taskManager.taskInfo.isInitialized
  /**
    * 暂存的entryPosition
    */
  var entryPosition: Option[EntryPosition] = None
  var fetchDelay: Long = taskManager.fetchDelay.get()
  var count = 0

  //offline
  override def receive: Receive = {

    case FetcherMessage(msg) => {
      msg match {
        case "restart" => {
          log.info(s"fetcher restarting,id:$syncTaskId")
          self ! SyncControllerMessage("start")
        }
        case str => {
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
            entryPosition = mysqlConnection
              .map(_.fork)
              .map {
                conn =>
                  conn.connect();
                  logPositionHandler.findStartPosition(conn)
              }
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
                if (!isInitialized) initEventualSinkSchema
                context.become(online)
                log.info(s"fetcher switch to online,id:$syncTaskId")
                self ! FetcherMessage("start")
            }
          }
          catch {
            case e: Exception => processError(e, SyncControllerMessage("start"))
          }

        }
        case _ => log.warning(s"fetcher offline  unhandled message:$msg,id:$syncTaskId")
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
          val startPosition = entryPosition.fold(throw new CannotFindOffsetException(s"fetcher find entryPosition is null,id:$syncTaskId"))(x => x)
          try {
            if (StringUtils.isEmpty(startPosition.getJournalName) && Option(startPosition.getTimestamp).isEmpty) {
              log.error(s"unsupported operation: dump by timestamp is not supported yet,id:$syncTaskId")
              throw new UnsupportedOperationException(s"unsupported operation: dump by timestamp is not supported yet,id:$syncTaskId")
            } else {
              log.info(s"start dump binlog,id:$syncTaskId")

              mysqlConnection.fold(throw new NullOfDataSourceConnectionException(s"mysqlConnection is null when start to dump,id:$syncTaskId"))(conn => MysqlConnection.dump(startPosition.getJournalName, startPosition.getPosition)(conn))
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
              entryPosition = Option(mysqlConnection.fold(throw new NullOfDataSourceConnectionException(s"mysqlConnection is null when find Start PositionWithinTransaction,id:$syncTaskId"))(conn => logPositionHandler.findStartPositionWithinTransaction(conn)))
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
    case SyncControllerMessage("pause") => {
      mysqlConnection.map(_.disconnect()) //断开数据库链接
      context.unbecome() //切回offline模式
      fetcherChangeStatus(Status.SUSPEND)
    }
  }


  /**
    * 用于在最终的数据汇建立相应的schema信息
    * 本次程序采用HBase作为最终数据汇
    * 0.检查传入的DatabaseList是否有问题
    * 1.检查最终数据汇的schema信息，如果没有初始化
    *   1.1 从mysql中获取当前元数据信息
    *   1.2 在HBase中创建表
    *   1.3 把信息更新到保存mysql元数据信息的数据库中
    * 2.将初始化标志置为true（防止重启时重复操作）
    **/
  override def initEventualSinkSchema: Unit = {
    val check = concernedDatabaseList.diff(taskManager.mysqlDatabaseNameList)
    if (!check.isEmpty) throw new ConcernedDatabaseCannotFoundException(s"database(s):${check.mkString(",")} cannot be found when init Eventual Sink Schema,$syncTaskId ")
    lazy val eventualSinkSchemaHandler = taskManager.eventualSinkSchemaHandler
    concernedDatabaseList
      //      .withFilter(!mysqlSchemaHandler.isInitialized(_))
      .map {
      dbName =>
        if (!mysqlSchemaHandler.isInitialized(dbName)) {
          mysqlSchemaHandler
            .getAllTablesInfo(dbName) match {
            case Failure(e) => throw new RetrieveSchemaFailureException(s"cannot fetch mysql schema,id:$syncTaskId", e)
            case Success(tableSchemas) => {

              eventualSinkSchemaHandler.createIfNotExists(HBaseTableInfo(dbName, ""))
              tableSchemas.map {
                kv =>
                  lazy val tableName = kv._1
                  lazy val schemas = kv._2
                  eventualSinkSchemaHandler.createIfNotExists(HBaseTableInfo(dbName, tableName))
                  schemas.map(schema => mysqlSchemaHandler.upsertSchema(schema, true))
              }
            }
          }
        } else {
            //已经初始化了的话
             eventualSinkSchemaHandler.createCache(dbName)
        }

    }

  }

  /**
    * 从连接中取数据
    */
  def fetchOne(before: Long = System.currentTimeMillis()) = {

    val entry = mysqlConnection.fold(throw new NullOfDataSourceConnectionException(s"mysqlConnection is null when fetch data,id:$syncTaskId"))(conn => conn.fetchUntilDefined(filterEntry(_))(binlogParser).get)
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

    entryOption.fold(false) {
      entry =>
        //我们只要rowdata和Transactioned
        if ((entry.getEntryType == CanalEntry.EntryType.ROWDATA) || (entry.getEntryType == CanalEntry.EntryType.TRANSACTIONEND)) true else false
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
  override def changeFunc(status: Status): Unit = TaskManager.changeFunc(status, taskManager)

  override def onChangeFunc: Unit = TaskManager.onChangeStatus(taskManager)

  override def fetcherChangeStatus(status: Status): Unit = TaskManager.changeStatus(status, changeFunc, onChangeFunc)

  /**
    * ********************* Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    log.info(s"fetcher switch to offline,id:$syncTaskId")
    mysqlConnection.map(conn => if (conn.isConnected) conn.disconnect())
    context.actorOf(MysqlBinlogInOrderFetcherCounter.props(taskManager), "counter")
    //状态置为offline
    fetcherChangeStatus(Status.OFFLINE)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"fetcher processing postRestart,id:$syncTaskId")
    super.postRestart(reason)

  }

  override def postStop(): Unit = {
    log.info(s"fetcher processing postStop,id:$syncTaskId")
    mysqlConnection.map(conn => if (conn.isConnected) conn.disconnect())
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

