package com.neighborhood.aka.laplce.estuary.mysql.lifecycle

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.position.{EntryPosition, LogPosition}
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle.{SinkerMessage, SourceDataSinker, Status, SyncControllerMessage}
import com.neighborhood.aka.laplce.estuary.mysql.{CanalEntryJsonHelper, Mysql2KafkaTaskInfoManager}
import org.I0Itec.zkclient.exception.ZkTimeoutException

/**
  * Created by john_liu on 2018/2/9.
  */
class ConcurrentBinlogSinker(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,binlogPositionRecorder:ActorRef) extends Actor with SourceDataSinker {

  val kafkaSinker = mysql2KafkaTaskInfoManager.kafkaSink
  val logPositionHandler = mysql2KafkaTaskInfoManager.logPositionHandler

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
    case list: List[CanalEntry.Entry] => {
      //todo log
      //todo 将entryList变成json
      //todo 探讨异步写入
//     val sinkFutureList =  list
//        .map {
//          x =>
//            (x, kafkaSinker
//              .sink("ssss")
//             // CanalEntryJsonHelper.entryToJson(x)
//            )
//        }
       list
         .map(CanalEntryJsonHelper.entryToJson(_))
         .foreach(println(_))

    }
    case x => {
      //todo log
      println(s"sinker online unhandled message $x")

    }
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
  def prop(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,binlogPositionRecorder: ActorRef): Props = {
    Props(new ConcurrentBinlogSinker(mysql2KafkaTaskInfoManager,binlogPositionRecorder))
  }
}