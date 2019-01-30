package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.SinkerMessage
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinkerCommand.MysqlInOrderSinkerGetAbnormal
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp

import scala.annotation.tailrec
import scala.util.{Success, Try}

/**
  * Created by john_liu on 2019/1/30.
  * 只负责写数据的Sinker
  * 将写数据的核心逻辑抽出
  *
  * @author neighborhood.aka.laplace
  */
final private[sink] class SimpleSinker(
                                        override val taskManager: MysqlSinkManagerImp with TaskManager,
                                        override val num: Int = -1
                                      ) extends MysqlBinlogInOrderSinker[MysqlSinkFunc, SqlList](taskManager) {


  var lastBinlogPosition: Option[BinlogPositionInfo] = None

  lazy val positionRecorder: Option[ActorRef] = taskManager.positionRecorder

  /**
    * 资源管理器
    */
  override val sinkManger: SinkManager[MysqlSinkFunc] = taskManager

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId


  /**
    * 错位次数阈值
    */
  override val errorCountThreshold: Int = 3


  /**
    * 错位次数
    */
  override var errorCount: Int = 0

  override def receive: Receive = {
    case m@SinkerMessage(x: SqlList) => {
      handleSinkTask(x).failed.foreach(processError(_, m))
      sendCount(x.shouldCount)
      sendCost((System.currentTimeMillis() - x.ts) / x.shouldCount)
    }
  }


  /**
    * 处理Batcher转换过的数据
    *
    * @param input batcher转换完的数据
    * @tparam I 类型参数 逆变
    */
  override protected def handleSinkTask[I <: SqlList](input: I): Try[_] = {
    lastBinlogPosition = input.binlogPositionInfo
    sinkFunc.insertBatchSql(input.list)
  }

  /**
    * 处理批量Batcher转换过的数据
    *
    * @param input batcher转换完的数据集合
    * @tparam I 类型参数 逆变
    */
  override protected def handleBatchSinkTask[I <: SqlList](input: List[I]): Try[_] = ???

  /**
    * 错误处理
    */
  @tailrec
  override final def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {
    e.printStackTrace() //打印异常
    log.warning(s"sinker went wrong,sqls:${message.msg},e:$e,${e.getCause},${e.getMessage},id:$syncTaskId")
    errorCount = errorCount + 1 //错误次数+1
    if (errorCount > errorCountThreshold) {
      lazy val positionInfo = Try(message.msg.asInstanceOf[SqlList].binlogPositionInfo) match {
        case Success(x) => x
        case _ => lastBinlogPosition
      }
      positionRecorder.fold(log.error(s"cannot find positionRecorder when sinker$num throw error,id:$syncTaskId")) {
        ref => ref ! SinkerMessage(MysqlInOrderSinkerGetAbnormal(e, positionInfo))
      }
      context.become(error, true)
    } else {
      Thread.sleep(SettingConstant.FAILURE_RETRY_BACKOFF)
      if (handleSinkTask(message.msg.asInstanceOf[SqlList]).isFailure) processError(e, message)
      else errorCount = 0
    }
  }
}

object SimpleSinker {
  def props(taskManager: MysqlSinkManagerImp with TaskManager, num: Int): Props = Props(new SimpleSinker(taskManager, num))
}