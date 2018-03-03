package com.neighborhood.aka.laplce.estuary.mysql.lifecycle

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import com.neighborhood.aka.laplce.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplce.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle.{SinkerMessage, SourceDataSinker, Status, SyncControllerMessage}
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager
import com.sun.scenario.effect.Offset
import org.I0Itec.zkclient.exception.ZkTimeoutException
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

/**
  * Created by john_liu on 2018/2/9.
  */
class ConcurrentBinlogSinker(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager, binlogPositionRecorder: ActorRef) extends Actor with SourceDataSinker {
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
    * 待保存的BinlogOffset
    */
  var lastSavedOffset: Long = _
  /**
    * 待保存的Binlog文件名称
    */
  var lastSavedJournalName: String = _

  //offline
  override def receive: Receive = {
    case SyncControllerMessage(msg) => {
      msg match {
        case "start" => {
          //online模式
          context.become(online)
          switch2Busy
        }
      }
    }
  }

  def online: Receive = {
    case list: List[Any] => {
      /**
        * 待保存的BinlogOffset
        */
      var savedOffset: Long = _
      /**
        * 待保存的Binlog文件名称
        */
      var savedJournalName: String = _
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

    }
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

        //todo 记录position
        if (exception != null) throw new RuntimeException("Error when send :" + key + ", metadata:" + metadata, exception)
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
        Restart
        //todo log
      }
      case e: Exception => {
        switch2Error
        Restart
      }
      case error: Error => Restart
      case _ => Restart
    }
  }
}

object ConcurrentBinlogSinker {
  def prop(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager, binlogPositionRecorder: ActorRef): Props = {
    Props(new ConcurrentBinlogSinker(mysql2KafkaTaskInfoManager, binlogPositionRecorder))
  }


}