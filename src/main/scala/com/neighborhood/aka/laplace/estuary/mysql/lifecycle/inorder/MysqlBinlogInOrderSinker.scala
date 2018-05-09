package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.{Actor, ActorLogging, Props}
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SinkerMessage, SourceDataSinker}
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/5/8.
  */
class MysqlBinlogInOrderSinker(
                                val mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,
                                val num: Int = -1
                              ) extends Actor with SourceDataSinker with ActorLogging {

  val syncTaskId = mysql2KafkaTaskInfoManager.syncTaskId
  val kafkaSinkFunc = mysql2KafkaTaskInfoManager.kafkaSink.fork
  val isSyncWrite = mysql2KafkaTaskInfoManager.taskInfo.isSync
  lazy val powerAdapter = mysql2KafkaTaskInfoManager.powerAdapter
  lazy val processingCounter = mysql2KafkaTaskInfoManager.processingCounter
  val isCounting = mysql2KafkaTaskInfoManager.taskInfo.isCounting
  val isCosting = mysql2KafkaTaskInfoManager.taskInfo.isCounting

  override def receive: Receive = {
    case message: KafkaMessage => handleSinkTask(message)
  }

  def handleSinkTask(message: KafkaMessage, isSync: Boolean = this.isSyncWrite): Unit = {
    val before = System.currentTimeMillis()
    lazy val after = System.currentTimeMillis()
    val seq = message.getBaseDataJsonKey.syncTaskSequence
    val tableName = message.getBaseDataJsonKey.tableName
    val dbName = message.getBaseDataJsonKey.dbName
    val key = if (seq <= 0) "DDL" else s"$dbName.$tableName"
    val topic = kafkaSinkFunc.findTopic(key)
    message.getBaseDataJsonKey.setKafkaTopic(topic)

    if (isSync) {
      //同步写
      kafkaSinkFunc.sink(message.getBaseDataJsonKey, message.getJsonValue)(topic)
    } else {
      log.error(s"暂时不支持异步写模式,id:$syncTaskId")
      throw new UnsupportedOperationException(s"暂时不支持异步写模式,id:$syncTaskId")
    }
    if (isCounting) powerAdapter.fold {}(ref => ref ! SinkerMessage(1))
    if (isCosting) processingCounter.fold {}(ref => ref ! SinkerMessage(after - before))
    //todo 日志中增加操作类型
    log.debug(s"sink primaryKey:${message.getBaseDataJsonKey.msgSyncUsedTime},id:$syncTaskId")
  }

  override def preStart(): Unit = {

    log.info(s"init sinker$num,id:$syncTaskId")

  }

  override def postStop(): Unit = {
    log.info(s"sinker$num processing postStop,id:$syncTaskId")
    kafkaSinkFunc.kafkaProducer.close()
    //    sinkTaskPool.environment.shutdown()
    //logPositionHandler.logPositionManage
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"sinker$num processing preRestart,id:$syncTaskId")
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"sinker$num processing preRestart,id:$syncTaskId")
    super.postRestart(reason)
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

object MysqlBinlogInOrderSinker {
  def props(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,
            num: Int = -1): Props = Props(new MysqlBinlogInOrderSinker(mysql2KafkaTaskInfoManager))
}