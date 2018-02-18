package com.neighborhood.aka.laplce.estuary.mysql.actors

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, OneForOneStrategy, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.position.{EntryPosition, LogPosition}
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle.{SourceDataSinker, SyncControllerMessage}
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager
import org.I0Itec.zkclient.exception.ZkTimeoutException

/**
  * Created by john_liu on 2018/2/9.
  */
class ConcurrentBinlogSinker(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) extends Actor with SourceDataSinker {

  val kafkaSinker = mysql2KafkaTaskInfoManager.kafkaSink
  val logPositionHandler = mysql2KafkaTaskInfoManager.logPositionHandler

  //offline
  override def receive: Receive = {
    case SyncControllerMessage(msg) => {
      msg match {
        case "start" => {
          context.become(online)
        }
      }
    }
  }

  def online: Receive = {
    case list: List[CanalEntry.Entry] => {
      //todo log
      //todo 将entryList变成json
      //todo 探讨异步写入
      kafkaSinker.sink(list.toString)
      //todo 异常处理
      val postion = new LogPosition
      val binlogFileName = list.head.getHeader.getLogfileName
      val offset = list.head.getHeader.getLogfileOffset
      val entryPostion = new EntryPosition(binlogFileName,offset)
      postion.setPostion(entryPostion)
      //todo 不确定是不是需要set Identity
      //写入zookeeper
      logPositionHandler.persistLogPosition(mysql2KafkaTaskInfoManager.taskInfo.syncTaskId, postion)


    }
  }

  override var errorCountThreshold: Int = 3
  override var errorCount: Int = 0

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???

  /**
    * ********************* 状态变化 *******************
    */
  /**
    * **************** Actor生命周期 *******************
    */
  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: ZkTimeoutException => {
        Restart
        //todo log
      }
      case e: Exception => {
        Restart
      }
      case error:Error => Restart
      case _ => Restart
    }
  }
}

object ConcurrentBinlogSinker {
  def prop(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager): Props = {
    Props(new ConcurrentBinlogSinker(mysql2KafkaTaskInfoManager))
  }
}