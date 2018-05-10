package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, FetcherMessage, SourceDataBatcher, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{BinlogPositionInfo, IdClassifier}
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.utils.{CanalEntry2KafkaMessageMappingFormat, CanalEntryJsonHelper}
import org.springframework.util.StringUtils

/**
  * Created by john_liu on 2018/5/8.
  */
class MysqlBinlogInOrderBatcher(
                                 val mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,
                                 val sinker: ActorRef,
                                 val num: Int = -1,
                                 val isDdlHandler: Boolean = false
                               ) extends Actor with SourceDataBatcher with ActorLogging with CanalEntry2KafkaMessageMappingFormat {


  override val syncTaskId: String = mysql2KafkaTaskInfoManager.syncTaskId
  lazy val powerAdapter = mysql2KafkaTaskInfoManager.powerAdapter
  lazy val processingCounter = mysql2KafkaTaskInfoManager.processingCounter

  /**
    * 该mysql实例所有的db
    */
  lazy val mysqlDatabaseNameList = MysqlConnection.getSchemas(mysql2KafkaTaskInfoManager.mysqlConnection.fork)

  /**
    * 是否计数
    */
  val isCounting = mysql2KafkaTaskInfoManager.taskInfo.isCounting

  /**
    * 是否计时
    */
  val isCosting = mysql2KafkaTaskInfoManager.taskInfo.isCosting
  val concernedDbName = mysql2KafkaTaskInfoManager.taskInfo.concernedDatabase
  val ignoredDbName = mysql2KafkaTaskInfoManager.taskInfo.ignoredDatabase


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
      },id:$syncTaskId")

      //性能分析
      if (isCosting) powerAdapter.fold(log.warning(s"cannot find processCounter,id:$syncTaskId"))(ref => ref ! BatcherMessage(kafkaMessage.getBaseDataJsonKey.msgSyncUsedTime))
      if (isCounting) processingCounter.fold(log.warning(s"cannot find powerAdapter,id:$syncTaskId"))(ref => ref ! BatcherMessage(1))
    }
    case info: BinlogPositionInfo => {
      sinker ! info
      if (isCounting) processingCounter.fold(log.warning(s"cannot find powerAdapter,id:$syncTaskId"))(ref => ref ! BatcherMessage(1))
    }
    case SyncControllerMessage("check") => sendHeartBeats
    case x => {
      log.warning(s" batcher unhandled message，$x,$syncTaskId")
    }
  }

  def sendHeartBeats = {


    val concernedDbNames = if (StringUtils.isEmpty(concernedDbName)) Array.empty[String] else concernedDbName.split(",")
    val ignoredDbNames = if (StringUtils.isEmpty(ignoredDbName)) Array.empty[String] else ignoredDbName.split(",")

    /**
      *
      * @param dbNameList 需要发送dummyData的db名称列表
      * @param sinker     sinker的ActorRef
      *                   构造假数据并发送给sinker
      */
    def buildAndSendDummyKafkaMessage(dbNameList: Iterable[String])(sinker: ActorRef): Int = {

      val kafkaMessageList = dbNameList
        .map { dbName => CanalEntryJsonHelper.dummyKafkaMessage(dbName) }
        .map { message => sinker ! message }
      dbNameList.size
    }

    val size = (concernedDbNames.size > 0, ignoredDbNames.size > 0) match {
      case (true, _) => buildAndSendDummyKafkaMessage(concernedDbNames)(sinker)
      case (_, true) => buildAndSendDummyKafkaMessage(mysqlDatabaseNameList.diff(ignoredDbNames))(sinker)
      case (_, false) => buildAndSendDummyKafkaMessage(mysqlDatabaseNameList)(sinker)
    }
    //补充计数
    if (isCounting) processingCounter.fold(log.warning(s"cannot find powerAdapter,id:$syncTaskId")){ ref => ref ! BatcherMessage(size); ref ! FetcherMessage(size) }
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