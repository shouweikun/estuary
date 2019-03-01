package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.mysql

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.bean.exception.sink.SinkerAbnormalException
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.SinkerMessage
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.record.MysqlBinlogInOrderRecorderCommand.MysqlBinlogInOrderRecorderEnsurePosition
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinkerCommand.MysqlInOrderSinkerGetAbnormal
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.{MysqlBinlogInOrderSinker, SqlList}
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp

import scala.annotation.tailrec
import scala.util.Try

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
    if (input.list == 1) sinkFunc.insertSql(input.list.head)
    else sinkFunc.insertBatchSql(input.list)
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
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {
    val sqlList = message.msg.asInstanceOf[SqlList]
    log.error(s"sinker throws error:$e,${e.getCause}.${e.getMessage},sqlList:$sqlList,id:$syncTaskId")
    lazy val binlogPositionInfo: Option[BinlogPositionInfo] = sqlList.binlogPositionInfo
    val timeout = binlogPositionInfo.map { info =>
      lazy val curr = System.currentTimeMillis()
      curr - sqlList.ts > 2 * 60 * 1000 && (curr - info.timestamp > 2 * 60 * 1000)
    }.getOrElse(true)
    lazy val isNoCountError = Option(e.getMessage).map {
      message =>
        message.toLowerCase.startsWith("table") &&
          message.toLowerCase.contains("doesn't exist") &&
          (e.getSuppressed.exists(x => x.isInstanceOf[com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException]))
    }.getOrElse(false) //对于表不存在的情况，不予计数
    if (timeout) throw new SinkerAbnormalException(s"fatal,sinker process error,besides the binlogPositionInfo is null or is timeout ,IMPOSSIBLE,plz check,id:$syncTaskId")
    if (!isNoCountError) errorCount = errorCount + 1 //错误次数+1
    binlogPositionInfo.map {
      info => positionRecorder.fold(log.error(s"cannot find positionRecorder when sinker$num throw error,id:$syncTaskId"))(ref => ref ! SinkerMessage(MysqlBinlogInOrderRecorderEnsurePosition(info)))
    }
    if (errorCount > errorCountThreshold) {
      positionRecorder.fold(log.error(s"cannot find positionRecorder when sinker$num throw error,id:$syncTaskId")) {
        ref => ref ! SinkerMessage(MysqlInOrderSinkerGetAbnormal(e, binlogPositionInfo))
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