package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, WorkerMessage}
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.MysqlRowDataInfo
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinkerCommand.MysqlInOrderSinkerGetAbnormal
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by john_liu on 2019/1/14.
  * mysql的Sinker
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInOrderMysqlSinker(
                                           override val taskManager: MysqlSinkManagerImp with TaskManager,
                                           override val num: Int
                                         ) extends MysqlBinlogInOrderSinker[MysqlSinkFunc, MysqlRowDataInfo](taskManager) {

  lazy val positionRecorder: Option[ActorRef] = taskManager.positionRecorder

  /**
    * 处理Batcher转换过的数据
    *
    * @todo 处理重试
    * @param input batcher转换完的数据
    * @tparam I 类型参数 逆变
    */
  override protected def handleSinkTask[I <: MysqlRowDataInfo](input: I): Try[_] = this.sinkFunc.insertSql(input.sql).flatMap(r => if (r == 0) Success(r) else Failure(new java.sql.SQLException(s"estuary insert data failed,cause re ==$r,which is not 0,id:$syncTaskId")))

  /**
    * 处理批量Batcher转换过的数据
    *
    * @param input batcher转换完的数据集合
    * @tparam I 类型参数 逆变
    */

  override protected def handleBatchSinkTask[I <: MysqlRowDataInfo](input: List[I]): Try[_] = this.sinkFunc.insertBatchSql(input.map(_.sql)).flatMap(r => if (r.exists(x => x != 0)) Failure(new java.sql.SQLException(s"estuary insert batch data failed,cause contains re ==$r,which is not 0,id:$syncTaskId")) else Success(r))


  override def receive: Receive = {
    case m@BatcherMessage(x: MysqlRowDataInfo) => {
      val startTime = System.currentTimeMillis()
      //      handleSinkTask(x).failed.foreach(processError(_, m))
      sendCost(System.currentTimeMillis() - startTime)
      sendCount(1)
    }
    case m@BatcherMessage(list: List[_]) if (list.head.isInstanceOf[MysqlRowDataInfo]) => {
      val startTime = System.currentTimeMillis()
      handleBatchSinkTask(list.asInstanceOf[List[MysqlRowDataInfo]]).failed.map(processError(_, m))
      sendCost((System.currentTimeMillis() - startTime) / list.size)
      sendCount(list.size)
    }
  }


  /**
    * 错位次数阈值
    */
  override val errorCountThreshold: Int = 1

  /**
    * 错位次数
    */
  override var errorCount: Int = 0

  /**
    * 错误处理
    *
    */
  @tailrec
  override final def processError(e: Throwable, message: WorkerMessage): Unit = {

    errorCount = errorCount + 1
    if (errorCount > errorCountThreshold) {
      positionRecorder.fold(log.error(s"cannot find positionRecorder when sinker$num throw error,id:$syncTaskId")) { ref => ref ! MysqlInOrderSinkerGetAbnormal(e, Try(message.msg.asInstanceOf[MysqlRowDataInfo].binlogPositionInfo).toOption) }
      context.become(error, true)
    }

    e.printStackTrace()
    log.warning(s"sinker$num process error:$e.message:$message,id:$syncTaskId")
    //这么写的原因是想转化成尾递归
    val result = message match {
      case BatcherMessage(x: MysqlRowDataInfo) => handleSinkTask(x)
      case BatcherMessage(list: List[_]) if (list.head.isInstanceOf[MysqlRowDataInfo]) => handleBatchSinkTask(list.asInstanceOf[List[MysqlRowDataInfo]])
    }
    if (result.isFailure) processError(e, message) else errorCount = 0
  }
}

object MysqlBinlogInOrderMysqlSinker {

  lazy val name = MysqlBinlogInOrderMysqlSinkerManager.getClass.getName.stripSuffix("$")

  def props(taskManager: MysqlSinkManagerImp with TaskManager, num: Int): Props = Props(new MysqlBinlogInOrderMysqlSinker(taskManager, num))
}

