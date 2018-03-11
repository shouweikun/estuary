package com.neighborhood.aka.laplce.estuary.mysql.lifecycle

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, AllForOneStrategy, OneForOneStrategy, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle._
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager
import org.I0Itec.zkclient.exception.ZkTimeoutException

import scala.concurrent.Future

/**
  * Created by john_liu on 2018/2/27.
  *
  */
@deprecated
class MysqlBinlogPositionRecorder(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) extends Actor with PositionRecorder {

  type SinkFutureList = List[(CanalEntry.Entry, Future[Boolean])]

  val logPositionHandler = mysql2KafkaTaskInfoManager.logPositionHandler
  val destination = mysql2KafkaTaskInfoManager.taskInfo.syncTaskId


  override def receive: Receive = {
    case SyncControllerMessage(x) => {
      x match {
        case "start" => {

        }
      }
    }
    case RecorderMessage(x) => {
      x match {
        case  "record" => {

        }
      }
    }
    case SinkerMessage(x) => {
      x match {
        case "error" => {
         // throw new RuntimeException("sinker has something wrong")
        }
        case _ =>{}
      }
    }
    case BinlogPositionInfo(journalName, offset) => {
      logPositionHandler.persistLogPosition(destination,journalName,offset)
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

  private def switch2Offline = {
    mysql2KafkaTaskInfoManager.sinkerStatus = Status.OFFLINE
  }

  private def switch2Error = {
    mysql2KafkaTaskInfoManager.sinkerStatus = Status.ERROR
  }

  private def switch2Online = {
    mysql2KafkaTaskInfoManager.sinkerStatus = Status.ONLINE
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
        Escalate
      }
      case error: Error => Escalate
      case _ => Escalate
    }
  }
}

object MysqlBinlogPositionRecorder {
  def props(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager):Props = {
    Props(new MysqlBinlogPositionRecorder(mysql2KafkaTaskInfoManager))
  }
}
