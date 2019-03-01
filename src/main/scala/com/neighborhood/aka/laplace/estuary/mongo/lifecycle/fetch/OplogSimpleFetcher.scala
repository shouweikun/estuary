package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.SyncControllerMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.DataSourceFetcherPrototype
import com.neighborhood.aka.laplace.estuary.core.task.{SourceManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch.OplogFetcherCommand._
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.util.OplogOffsetHandler
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoSourceManagerImp}

/**
  * Created by john_liu on 2019/2/28.
  *
  * 處理oplog 拉取的actor
  *
  * @author neighborhood.aka.laplace
  */
final class OplogSimpleFetcher(
                                val taskManager: MongoSourceManagerImp with TaskManager,
                                val downStream: ActorRef
                              ) extends DataSourceFetcherPrototype[MongoConnection] {
  /**
    * 数据源资源管理器
    */
  override val sourceManager: SourceManager[MongoConnection] = taskManager

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

  val isCounting = taskManager.isCounting

  lazy val powerAdapter = taskManager.powerAdapter

  lazy val processingCounter = taskManager.processingCounter

  val batchNum: Long = taskManager.batchThreshold
  /**
    * 位置处理器
    */
  private lazy val logPositionHandler: OplogOffsetHandler = taskManager.positionHandler
  private lazy val simpleFetchModule: SimpleFetchModule = {
    val offset = Option(logPositionHandler.findStartPosition(connection))
    new SimpleFetchModule(connection, offset, syncTaskId)
  }

  private var lastFetchTimestamp = System.currentTimeMillis()


  override def receive: Receive = {
    case SyncControllerMessage(MysqlBinlogInOrderFetcherStart) => start
    case SyncControllerMessage(MysqlBinlogInOrderFetcherFetch) => handleFetchTask
  }

  private def start: Unit = {
    simpleFetchModule
  }

  private def handleFetchTask: Unit = {
    simpleFetchModule.fetch.foreach {
      doc =>
        val curr = System.currentTimeMillis()
        sendData(doc)
        lastFetchTimestamp = curr
    }
  }

  private def sendData(data: Any) = downStream ! data

  /**
    * 错位次数阈值
    */
  override def errorCountThreshold: Int = 3

  /**
    * 错位次数
    */
  override var errorCount: Int = 0

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???
}
