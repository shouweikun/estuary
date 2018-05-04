package com.neighborhood.aka.laplace.estuary.mongo.lifecycle

import java.util.concurrent.Executors

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.mongodb.DBObject
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{FetcherMessage, SourceDataFetcher, Status, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.util.JavaCommonUtil
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoOffset}
import com.neighborhood.aka.laplace.estuary.mongo.task.Mongo2KafkaTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mongo.utils.MongoOffsetHandler

import scala.concurrent.ExecutionContext

/**
  * Created by john_liu on 2018/4/26.
  *
  * 1.首先寻找开始位点
  * 2.开始拉取数据
  */
class OplogFetcher(
                    val mongo2KafkaTaskInfoManager: Mongo2KafkaTaskInfoManager,
                    val oplogBatcher: ActorRef
                  )
  extends SourceDataFetcher with Actor with ActorLogging {

  implicit val transTaskPool = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  /**
    * 寻址处理器
    */
  lazy val mongoOffsetHandler: MongoOffsetHandler = mongo2KafkaTaskInfoManager.mongoOffsetHandler

  lazy val mongoConnection: MongoConnection = mongo2KafkaTaskInfoManager.mongoConnection.fork

  lazy val oplogIterator: java.util.Iterator[DBObject] = {
    mongoOffset
      .fold(throw new Exception(s"connot find start mongoOffset when preparing query,id:$syncTaskId "))(mongoConnection.QueryOplog(_))
      .iterator()
  }
  /**
    * 是否记录耗时
    */
  val isCosting: Boolean = mongo2KafkaTaskInfoManager.taskInfoBean.isCosting
  /**
    * 是否计数
    */
  val isCounting: Boolean = mongo2KafkaTaskInfoManager.taskInfoBean.isCounting

  val syncTaskId: String = mongo2KafkaTaskInfoManager.syncTaskId
  /**
    * 暂存的MongoOffset
    */
  var mongoOffset: Option[MongoOffset] = None
  /**
    * 拉取数据延时
    */
  var fetchDelay: Int = 0
  /**
    * 是否处于拉取数据状态
    */
  var isFetching: Boolean = true

  override def receive: Receive = {
    case SyncControllerMessage(msg) => {
      msg match {
        case "start" => {
          mongoOffset = Option(mongoOffsetHandler.findStartPosition(mongoConnection.fork))
          mongoOffset.fold {
            log.error(s"connot find start mongoOffset,id:$syncTaskId")
            throw new Exception(s"connot find start mongoOffset,id:$syncTaskId")
          } {
            theMongoOffset =>
              log.info(s"find start mongoOffset:$mongoOffset,id:$syncTaskId")
              context.become(online)
              self ! FetcherMessage("start")
          }
        }
        case x => log.warning(s"oplog fetcher id:$syncTaskId offline unhandled message ${SyncControllerMessage(msg)}")
      }
    }
  }


  def online: Receive = {
    case FetcherMessage(msg) => {
      msg match {
        case "start" => {
          changeFunc(Status.ONLINE)
          log.info(s"oplog fetcher id:$syncTaskId switch to online")
          self ! FetcherMessage("prepareQuery")
        }
        case "prepareQuery" => {
          log.info(s"oplog fetcher id:$syncTaskId is Preparing Query ")
          oplogIterator
          self ! FetcherMessage("fetch")
        }
        case "fetch" => {
          if (isFetching) fetch else {
            log.warning(s"downstream is too busy,suspend to fetch data id:$syncTaskId")
          }
        }
        case x => log.warning(s"oplog fetcher id:$syncTaskId offline unhandled message ${FetcherMessage(x)}")
      }
    }
    case SyncControllerMessage(msg) => {
      msg match {
        case x if (JavaCommonUtil.isInteger(x)) => fetchDelay = x.toInt
        case "suspend" => {
          //未被使用
          isFetching = false
          log.info(s"fetcher suspended, id:$syncTaskId")
        }
        case "resume" => {
          //未被使用
          isFetching = true
          log.info(s"fetcher resumed, id:$syncTaskId")
        }
        case x => log.warning(s"oplog fetcher id:$syncTaskId offline unhandled message ${SyncControllerMessage(x)}")
      }
    }
    case _ =>
  }

  def fetch = {
    if (oplogIterator.hasNext) {
      val oplog = oplogIterator.next()
      oplogBatcher ! oplog
      log.debug(s"fetcher fetchs oplog _id:${oplog.get("_id")},id:$syncTaskId")
    } else {
//      oplogBatcher ! FetcherMessage("none")
      log.debug(s"fetcher fetchs no oplog,id:$syncTaskId")
    }
  }

  /**
    * 错位次数阈值
    */
  override var errorCountThreshold: Int = _
  /**
    * 错位次数
    */
  override var errorCount: Int = _

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???

  /**
    * ********************* 状态变化 *******************
    */
  private def changeFunc(status: Status): Unit = TaskManager.changeFunc(status, mongo2KafkaTaskInfoManager)

  private def onChangeFunc: Unit = TaskManager.onChangeStatus(mongo2KafkaTaskInfoManager)

  private def fetcherChangeStatus(status: Status): Unit = TaskManager.changeStatus(status, changeFunc, onChangeFunc)
}
