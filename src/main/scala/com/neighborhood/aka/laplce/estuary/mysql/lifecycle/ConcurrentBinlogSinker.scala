package com.neighborhood.aka.laplce.estuary.mysql.lifecycle

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, Props}
import com.neighborhood.aka.laplce.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplce.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle.{SinkerMessage, SourceDataSinker, Status, SyncControllerMessage}
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager
import org.I0Itec.zkclient.exception.ZkTimeoutException
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.springframework.util.StringUtils

/**
  * Created by john_liu on 2018/2/9.
  */
class ConcurrentBinlogSinker(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) extends Actor with SourceDataSinker with ActorLogging{
  /**
    * 拼接json用
    */
  private val START_JSON = "{"
  private val END_JSON = "}"
  private val START_ARRAY = "["
  private val END_ARRAY = "]"
  private val KEY_VALUE_SPLIT = ":"
  private val ELEMENT_SPLIT = ","
  private val STRING_CONTAINER = "\""
  /**
    * kafkaSinker
    */
  val kafkaSinker = mysql2KafkaTaskInfoManager.kafkaSink
  /**
    * logPosition处理
    */
  val logPositionHandler = mysql2KafkaTaskInfoManager.logPositionHandler
  /**
    * 同步标识作为写入zk 的标识
    */
  val destination = mysql2KafkaTaskInfoManager.taskInfo.syncTaskId
  /**
    * 本次同步任务开始的logPosition
    * 从zk中获取
    */
  val startPosition = Option(logPositionHandler.logPositionManager.getLatestIndexBy(destination))
  /**
    * 待保存的BinlogOffset
    */
  var lastSavedOffset: Long = if (startPosition.isDefined) {
    startPosition.get.getPostion.getPosition
  } else 4L
  /**
    * 待保存的Binlog文件名称
    */
  var lastSavedJournalName: String = if (startPosition.isDefined) {
    startPosition.get.getPostion.getJournalName
  } else ""

  //offline
  override def receive: Receive = {
    case SyncControllerMessage(msg) => {
      msg match {
        case "start" => {
          //online模式
          log.info("sinker swtich to online")
          context.become(online)
          switch2Busy
        }
        case x => {
         log.warning(s"sinker offline unhandled message:$x")
        }
      }
    }
  }

  def online: Receive = {
    case list: List[Any] => {
      /**
        * 待保存的BinlogOffset
        */
      var savedOffset: Long = 0L
      /**
        * 待保存的Binlog文件名称
        */
      var savedJournalName: String = ""
      list
        .map {
          x =>
            x match {
              case message: KafkaMessage => {
                handleSinkTask(message)
              }
              case BinlogPositionInfo(journalName, offset) => {
                savedJournalName = journalName
                savedOffset = offset
              }
            }
        }

      //这次任务完成后
      //todo 探讨flush是否需要Future
      //kafka flush 数据
      kafkaSinker.flush
      //保存这次任务的binlog

      this.lastSavedJournalName = savedJournalName
      this.lastSavedOffset = savedOffset
      log.info(s"JournalName update to $savedJournalName,offset update to $savedOffset")
    }
    // 定时记录logPosition
    case SyncControllerMessage("record") => logPositionHandler.persistLogPosition(destination,lastSavedJournalName,lastSavedOffset)
    case x => {
      //todo log
      println(s"sinker online unhandled message $x")

    }
  }

  /**
    *
    */
  def handleSinkTask(kafkaMessage: KafkaMessage, journalName: String = this.lastSavedJournalName, offset: Long = this.lastSavedOffset) = {
    val key = s"${kafkaMessage.getBaseDataJsonKey.asInstanceOf[BinlogKey].getDbName}.${kafkaMessage.getBaseDataJsonKey.asInstanceOf[BinlogKey].getTableName}"
    val topic = kafkaSinker.findTopic(key)
    val callback = new Callback {

      val theOffset = lastSavedOffset
      val theJournalName = lastSavedJournalName

      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          //记录position
          if (!StringUtils.isEmpty(theJournalName)) {
            logPositionHandler.persistLogPosition(destination, theJournalName, theOffset)
          }
         log.error("Error when send :" + key + ", metadata:" + metadata, exception)
          //扔出异常，让程序感知
          throw new RuntimeException("Error when send :" + key + ", metadata:" + metadata, exception)
        } else {
          log.info("send success:" + key + ", metadata:" + metadata)
        }
      }
    }
    kafkaSinker.ayncSink(kafkaMessage.getBaseDataJsonKey.asInstanceOf[BinlogKey], kafkaMessage.getJsonValue)(topic)(callback)
  }


  override var errorCountThreshold: Int = 3
  override var errorCount: Int = 0

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {
    //do nothing
  }

  /**
    * ********************* 状态变化 *******************
    */

  private def switch2Offline = {
    mysql2KafkaTaskInfoManager.sinkerStatus = Status.OFFLINE
  }

  private def switch2Busy = {
    mysql2KafkaTaskInfoManager.sinkerStatus = Status.BUSY
  }

  private def switch2Error = {
    mysql2KafkaTaskInfoManager.sinkerStatus = Status.ERROR
  }

  private def switch2Free = {
    mysql2KafkaTaskInfoManager.sinkerStatus = Status.FREE
  }

  private def switch2Restarting = {
    mysql2KafkaTaskInfoManager.sinkerStatus = Status.RESTARTING
  }


  /**
    * **************** Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    switch2Offline

  }


  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.become(receive)
  }

  override def postRestart(reason: Throwable): Unit = {
    switch2Restarting
    context.parent ! SinkerMessage("restart")
    super.postRestart(reason)
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: ZkTimeoutException => {
        Escalate
        //todo log
      }
      case e: Exception => {
        switch2Error
        Escalate
      }
      case error: Error => Escalate
      case _ => Escalate
    }
  }
}

object ConcurrentBinlogSinker {
  //  def prop(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager, binlogPositionRecorder: ActorRef): Props = {
  //    Props(new ConcurrentBinlogSinker(mysql2KafkaTaskInfoManager, binlogPositionRecorder))

  def prop(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager): Props = {
    Props(new ConcurrentBinlogSinker(mysql2KafkaTaskInfoManager))
  }

}