package com.neighborhood.aka.laplce.estuary.mysql.actors

import java.util.concurrent.atomic.AtomicLong

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle._
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by john_liu on 2018/2/6.
  */
class BinlogEventBatcher(binlogEventSinker: ActorRef, mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) extends Actor with SourceDataBatcher {
  override var errorCountThreshold: Int = 3
  override var errorCount: Int = 0
  val mode = mysql2KafkaTaskInfoManager.taskInfo.isTransactional
  var entryBatch: List[CanalEntry.Entry] = List.empty
  var batchThreshold: AtomicLong = mysql2KafkaTaskInfoManager.taskInfo.batchThreshold

  //offline
  override def receive: Receive = {
    case SyncControllerMessage(msg) => {
      msg match {
        case "start" => {
          if (mode) {
            context.become(Transactional)
          }else {
            context.become(Parallizal)
          }
          switch2Busy
        }
      }
    }
    case BatcherMessage(msg) => {
      msg match {
        case "restart" => {
          switch2Restarting
          self ! SyncControllerMessage("start")
        }
      }
    }
  }

  /**
    * 事务模式
    */
  def Transactional: Receive = {
    case entry: CanalEntry.Entry => binlogEventSinker ! entry
  }

  /**
    * 并行写模式
    */
  def Parallizal: Receive = {
    case entry: CanalEntry.Entry => {
      batchAndFlush(entry)
    }
    case SyncControllerMessage(msg) => {
      msg match {
        case "flush" => flush
      }
    }
  }
  /**
    * @param entry Canalentry
    * 打包如果包内数量超过阈值刷新并发送给sinker
    */
  def batchAndFlush(entry: CanalEntry.Entry): Unit = {
    entryBatch = entryBatch.::(entry)
    if (entryBatch.size >= batchThreshold.get()) {
      flush
    }
  }
  /**
    *
    * 刷新list里的值并发送给sinker
    */
  def flush = {
    binlogEventSinker ! entryBatch
    entryBatch = List.empty
  }

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {
    //batcher 出错不用处理，让他直接崩溃
  }

  /**
    * ********************* 状态变化 *******************
    */
  private def switch2Offline = {
    mysql2KafkaTaskInfoManager.batcherStatus = Status.OFFLINE
  }

  private def switch2Busy = {
    mysql2KafkaTaskInfoManager.batcherStatus = Status.BUSY
  }

  private def switch2Error = {
    mysql2KafkaTaskInfoManager.batcherStatus = Status.ERROR
  }

  private def switch2Free = {
    mysql2KafkaTaskInfoManager.batcherStatus = Status.FREE
  }

  private def switch2Restarting = {
    mysql2KafkaTaskInfoManager.batcherStatus = Status.RESTARTING
  }

  /**
    * ********************* Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    //状态置为offline
    switch2Offline
    context.system.scheduler.scheduleOnce(3 minutes)(self ! FetcherMessage("restart"))

  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: Exception => Restart
      case _ => Escalate
    }
  }
}
object BinlogEventBatcher {
  def prop(binlogEventSinker: ActorRef, mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager): Props = Props(new BinlogEventBatcher(binlogEventSinker, mysql2KafkaTaskInfoManager))
}


