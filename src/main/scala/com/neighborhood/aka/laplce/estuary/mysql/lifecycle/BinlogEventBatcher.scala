package com.neighborhood.aka.laplce.estuary.mysql.lifecycle

import java.util.concurrent.atomic.AtomicLong

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle._
import com.neighborhood.aka.laplce.estuary.mysql.{Mysql2KafkaTaskInfoManager, MysqlBinlogParser}
import com.taobao.tddl.dbsync.binlog.LogEvent

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by john_liu on 2018/2/6.
  */
class BinlogEventBatcher(binlogEventSinker: ActorRef, mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) extends Actor with SourceDataBatcher {
  override var errorCountThreshold: Int = 3
  override var errorCount: Int = 0
  /**
    * binlogParser 解析binlog
    */
  lazy val binlogParser: MysqlBinlogParser = mysql2KafkaTaskInfoManager.binlogParser
  val mode = mysql2KafkaTaskInfoManager.taskInfo.isTransactional
  var entryBatch: List[CanalEntry.Entry] = List.empty
  var batchThreshold: AtomicLong = mysql2KafkaTaskInfoManager.taskInfo.batchThreshold

  //offline
  override def receive: Receive = {
    case SyncControllerMessage(msg) => {
      msg match {
        case "start" => {
          context.become(online)
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
    case entry: CanalEntry.Entry => {
      //todo log


    }
    case x => {
      //todo log
      println(s"BinlogBatcher unhandled Message : $x")
    }
  }

  /**
    * online
    */
  def online: Receive = {
    case entry: CanalEntry.Entry => {
      batchAndFlush(entry)()
    }
    case event: LogEvent => {
      tranformAndHandle(event)()
    }
    case SyncControllerMessage(msg) => {
      msg match {
        case "flush" => flush
      }
    }
  }

  /**
    * @param entry canalEntry
    *              打包如果包内数量超过阈值刷新并发送给sinker
    */
  def batchAndFlush(entry: CanalEntry.Entry)(mode: Boolean = this.mode): Unit = {
    if (mode) {
      binlogEventSinker ! entry
    } else {
      entryBatch = entryBatch.+:(entry)
      if (entryBatch.size >= batchThreshold.get()) {
        flush
      }
    }
  }

  /**
    * 刷新list里的值并发送给sinker
    */
  def flush = {
    if (!entryBatch.isEmpty) {
      binlogEventSinker ! entryBatch
      entryBatch = List.empty
    }
  }

  /**
    * entry 解码
    */
  private def transferEvent(event: LogEvent): Option[CanalEntry.Entry] = {
    binlogParser.parseAndProfilingIfNecessary(event, false)
  }

  /**
    * entry 解码并处理
    */
  def tranformAndHandle(event: LogEvent)(mode: Boolean = this.mode) = {
    val entry = transferEvent(event)
    (entry.isDefined, mode) match {
      //transactional模式
      case (true, true) => {
        //todo log
        binlogEventSinker ! entry.get
      }
      //concurrent模式
      case (true, false) => {
        //todo log
        println(s"${entry.get.getHeader.getExecuteTime}")
        batchAndFlush(entry.get)()
      }
      case (false, _) => {
        //todo log
        println(System.currentTimeMillis())
      }
    }
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

    context.system.scheduler.scheduleOnce(1 minutes)(self ! FetcherMessage("restart"))

  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case _ => Escalate
    }
  }
}

object BinlogEventBatcher {
  def prop(binlogEventSinker: ActorRef, mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager): Props = Props(new BinlogEventBatcher(binlogEventSinker, mysql2KafkaTaskInfoManager))
}


