package com.neighborhood.aka.laplce.estuary.mysql.lifecycle

import akka.actor.Actor
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle.{PositionRecorder, RecorderMessage, SinkerMessage, SyncControllerMessage}
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/2/27.
  */
class MysqlBinlogPositionRecorder(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) extends Actor with PositionRecorder {

  val logPositionHandler = mysql2KafkaTaskInfoManager.logPositionHandler
  override def receive: Receive = {
    case SyncControllerMessage(msg: String) => {
      msg match {
        case "start" => {
          //todo log
          context.become(online)
        }
      }
    }
  }
  //online
  def online: Receive = {
    case SyncControllerMessage(msg: String) => {
      msg match {
        case "record" => {
          //todo
        }
      }
    }
    case RecorderMessage(msg: String) => {
      msg
    }
    case SinkerMessage(msg:String) => {
      msg match {
        case x => {
          x.toLong
        }
      }
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
}
