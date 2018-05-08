package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, SourceDataBatcher}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder.MysqlBinlogInOrderBatcherManager.IdClassifier
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
      val kafkaMessage = transform(x)
      kafkaMessage.getBaseDataJsonKey.setSyncTaskSequence(num)
      sinker ! kafkaMessage
      log.debug(s"batch primaryKey:${
        x.consistentHashKey
      },id:$syncTaskId")

      //性能分析
      if (isCosting) powerAdapter.fold(log.warning(s"cannot find processCounter,id:$syncTaskId"))(ref => ref ! BatcherMessage(kafkaMessage.getBaseDataJsonKey.msgSyncUsedTime))
      if (isCounting) processingCounter.fold(log.warning(s"cannot find powerAdapter,id:$syncTaskId"))(ref => ref ! BatcherMessage(1))
    }
    case "check" => sendHeartBeats
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

      val kafkaMessageList: List[Any] = dbNameList
        .map {
          dbName =>
            CanalEntryJsonHelper.dummyKafkaMessage(dbName)
        }.toList
      sinker ! kafkaMessageList
      dbNameList.size
    }

    val size = (concernedDbNames.size > 0, ignoredDbNames.size > 0) match {
      case (true, _) => buildAndSendDummyKafkaMessage(concernedDbNames)(sinker)
      case (_, true) => buildAndSendDummyKafkaMessage(mysqlDatabaseNameList.diff(ignoredDbNames))(sinker)
      case (_, false) => buildAndSendDummyKafkaMessage(mysqlDatabaseNameList)(sinker)
    }
    //补充计数
    if (isCounting) processingCounter.fold(log.warning(s"cannot find powerAdapter,id:$syncTaskId"))(ref => ref ! BatcherMessage(size))
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
}

object MysqlBinlogInOrderBatcher {
  def props(
             mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,
             sinker: ActorRef,
             num: Int = -1,
             isDdlHandler: Boolean = false
           ): Props = Props(new MysqlBinlogInOrderBatcher(mysql2KafkaTaskInfoManager, sinker, num, isDdlHandler))
}