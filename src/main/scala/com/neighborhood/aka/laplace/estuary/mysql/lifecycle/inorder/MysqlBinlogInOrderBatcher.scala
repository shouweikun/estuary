package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.bean.exception.control.WorkerCannotFindException
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataBatcherPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, FetcherMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{BinlogPositionInfo, IdClassifier}
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.utils.{CanalEntry2KafkaMessageMappingFormat, CanalEntryJsonHelper}

import scala.concurrent.Promise

/**
  * Created by john_liu on 2018/5/8.
  */
class MysqlBinlogInOrderBatcher(
                                 override val taskManager: Mysql2KafkaTaskInfoManager,
                                 override val sinker: ActorRef,
                                 override val num: Int = -1,
                                 val isDdlHandler: Boolean = false
                               ) extends SourceDataBatcherPrototype[IdClassifier, KafkaMessage]
  with CanalEntry2KafkaMessageMappingFormat {

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId
  /**
    * 是否同步
    */
  override val isSync: Boolean = taskManager.isSync
  /**
    * 功率控制器
    */
  lazy val powerAdapter = taskManager.powerAdapter
  /**
    * 计数器
    */
  lazy val processingCounter = taskManager.processingCounter
  /**
    * 该mysql实例所有的db
    */
  lazy val mysqlDatabaseNameList = taskManager.mysqlDatabaseNameList
  /**
    * 是否计数
    */
  val isCounting = taskManager.taskInfo.isCounting

  /**
    * 是否计时
    */
  val isCosting = taskManager.taskInfo.isCosting
  /**
    * 需要发送心跳的数据库名称
    */
  val concernedDbName = taskManager.taskInfo.concernedDatabase
  /**
    * 不需要发送心跳的数据库名称
    */
  val ignoredDbName = taskManager.taskInfo.ignoredDatabase

  override def receive: Receive = {

    case x: IdClassifier => {

      //      implicit val num = this.num
      //      val before = System.currentTimeMillis()
      val kafkaMessage = transform(x)
      //      val after = System.currentTimeMillis()
      //      val a = after - before
      kafkaMessage.getBaseDataJsonKey.setSyncTaskSequence(num)
      sinker ! kafkaMessage
      log.debug(s"batch primaryKey:${
        x.consistentHashKey
      },eventType:${
        x.entry.getHeader.getEventType
      },id:$syncTaskId")

      //性能分析
      if (isCosting) powerAdapter.fold(throw new WorkerCannotFindException(s"cannot find processCounter,id:$syncTaskId")
      )(ref => ref ! BatcherMessage(kafkaMessage.getBaseDataJsonKey.msgSyncUsedTime))
      if (isCounting) processingCounter.fold(throw new WorkerCannotFindException(s"cannot find powerAdapter,id:$syncTaskId"))(ref => ref ! BatcherMessage(1))
    }
    case info: BinlogPositionInfo => {
      sinker ! info
      if (isCounting) processingCounter.fold(throw new WorkerCannotFindException(s"cannot find powerAdapter,id:$syncTaskId"))(ref => ref ! BatcherMessage(1))
    }
    case SyncControllerMessage("check") => sendHeartBeats
    case x => {
      log.warning(s" batcher unhandled message，$x,$syncTaskId")
    }
  }

  /**
    * 发送心跳
    */
  def sendHeartBeats: Unit = {

    def isEmpty(string: Any): Boolean = string == null || "".equals(string)

    val concernedDbNames = if (isEmpty(concernedDbName)) Array.empty[String] else concernedDbName.split(",")
    val ignoredDbNames = if (isEmpty(ignoredDbName)) Array.empty[String] else ignoredDbName.split(",")

    /**
      *
      * @param dbNameList 需要发送dummyData的db名称列表
      * @param sinker     sinker的ActorRef
      *                   构造假数据并发送给sinker
      */
    def buildAndSendDummyKafkaMessage(dbNameList: Iterable[String])(sinker: ActorRef): Int = {

      val kafkaMessageList = dbNameList
        .map {
          dbName => CanalEntryJsonHelper.dummyKafkaMessage(dbName)
        }
        .map {
          message => sinker ! message
        }
      dbNameList.size
    }

    val size = (concernedDbNames.size > 0, ignoredDbNames.size > 0) match {
      case (true, _) => buildAndSendDummyKafkaMessage(concernedDbNames)(sinker)
      case (_, true) => buildAndSendDummyKafkaMessage(mysqlDatabaseNameList.diff(ignoredDbNames))(sinker)
      case (_, false) => buildAndSendDummyKafkaMessage(mysqlDatabaseNameList)(sinker)
    }
    //补充计数
    if (isCounting) processingCounter.fold(log.warning(s"cannot find powerAdapter,id:$syncTaskId")) {
      ref =>
        ref ! BatcherMessage(size);
        ref ! FetcherMessage(size)
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

  override def preStart(): Unit = {
    log.info(s"init batcher$num,id:$syncTaskId")
    if (isDdlHandler) log.info(s"batcher0 is ddlHandler,id:$syncTaskId")
  }

  override def postStop(): Unit = {

  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"batcher$num process preRestart,id:$syncTaskId")
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"batcher$num process postRestart,id:$syncTaskId")
    super.postRestart(reason)
  }
}

object MysqlBinlogInOrderBatcher {
  def props(
             mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,
             sinker: ActorRef,
             num: Int = -1,
             isDdlHandler: Boolean = false
           ): Props = Props(new MysqlBinlogInOrderBatcher(mysql2KafkaTaskInfoManager, sinker, num, isDdlHandler))
}