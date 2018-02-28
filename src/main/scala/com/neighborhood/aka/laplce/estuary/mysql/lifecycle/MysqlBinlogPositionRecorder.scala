package com.neighborhood.aka.laplce.estuary.mysql.lifecycle

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, AllForOneStrategy, OneForOneStrategy, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle._
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager
import org.I0Itec.zkclient.exception.ZkTimeoutException

import scala.concurrent.Future

/**
  * Created by john_liu on 2018/2/27.
  */
class MysqlBinlogPositionRecorder(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) extends Actor with PositionRecorder {

  type SinkFutureList = List[(CanalEntry.Entry, Future[Boolean])]

  val logPositionHandler = mysql2KafkaTaskInfoManager.logPositionHandler
  val destination = mysql2KafkaTaskInfoManager.taskInfo.syncTaskId

  override def receive: Receive = {
    case SyncControllerMessage(msg: String) => {
      msg match {
        case "start" => {
          //todo log
          context.become(online)
        }
      }
    }
    case _ => {
      println("recorder unhandled message")
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
    case SinkerMessage(msg: String) => {
      msg match {
        case x => {

        }

      }
    }
    case list: SinkFutureList => {
      //todo 这块设计的不好
      val flag = list
        .forall {
          sinkFuture =>
            sinkFuture
              ._2
              .value
              .get
              .get
        }
      if (flag) {
        val entry = logPositionHandler.buildLastPosition(list.head._1)
        logPositionHandler.persistLogPosition(destination, entry)
      } else {
        throw new Exception("kafka 写入失败")
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
    AllForOneStrategy() {
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

object MysqlBinlogPositionRecorder {
  def props(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager):Props = {
    Props(new MySqlBinlogController(mysql2KafkaTaskInfoManager))
  }
}
