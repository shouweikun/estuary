package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, SinkerMessage, WorkerMessage}
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.util.{JavaCommonUtil, SimpleEstuaryRingBuffer}
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinkerCommand.{MysqlInOrderSinkerCheckBatch, MysqlInOrderSinkerGetAbnormal}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{BinlogPositionInfo, MysqlRowDataInfo}
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp

import scala.concurrent.duration._
import scala.util.{Success, Try}

/**
  * Created by john_liu on 2019/1/14.
  * mysql的Sinker
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInOrderMysqlRingBufferSinker(
                                                     override val taskManager: MysqlSinkManagerImp with TaskManager,
                                                     override val num: Int
                                                   ) extends MysqlBinlogInOrderSinker[MysqlSinkFunc, MysqlRowDataInfo](taskManager) {
  implicit val ec = context.dispatcher
  /**
    * 位置记录器
    */
  lazy val positionRecorder: Option[ActorRef] = taskManager.positionRecorder
  /**
    * ringBuffer
    */
  protected lazy val ringBuffer = new SimpleEstuaryRingBuffer[MysqlRowDataInfo](255) //支持batch

  private var lastBinlogPosition: Option[BinlogPositionInfo] = None

  /**
    * 处理Batcher转换过的数据
    *
    * @todo 处理重试
    * @param input batcher转换完的数据
    * @tparam I 类型参数 逆变
    */
  override protected def handleSinkTask[I <: MysqlRowDataInfo](input: I): Try[Unit] = putAndFlushWhenFull(input)

  /**
    * 处理批量Batcher转换过的数据
    *
    * @param input batcher转换完的数据集合
    * @tparam I 类型参数 逆变
    */

  override protected def handleBatchSinkTask[I <: MysqlRowDataInfo](input: List[I]): Try[_] = input.map(handleSinkTask(_)).foldLeft(Success(List.empty[Unit]): Try[List[Unit]]) { (t1, t2) => t1.map(x => t2.get :: x) }

  /**
    * 上次刷新时间
    *
    * 超过${Constant.SINKER_FLUSH_INTERVAL}才会刷新
    */
  var lastFlushTime: Long = 0

  override def receive: Receive = {
    case m@BatcherMessage(x: MysqlRowDataInfo) => putAndFlushWhenFull(x).failed.foreach(processError(_, m))
    case m@BatcherMessage(MysqlInOrderSinkerCheckBatch) => handleCheckFlush.failed.foreach(processError(_, m))
    case m@BatcherMessage(list: List[_]) if (list.head.isInstanceOf[MysqlRowDataInfo]) => handleBatchSinkTask(list.asInstanceOf[List[MysqlRowDataInfo]]).failed.foreach(processError(_, m))


  }

  /**
    * 向ringBuffer中添加元素，满了会触发flush
    *
    * @param x 待添加的元素
    * @return 刷新是否成功
    */
  private def putAndFlushWhenFull(x: MysqlRowDataInfo): Try[Unit] = Try {
    if (ringBuffer.isFull) flush
    ringBuffer.put(x)
    if (ringBuffer.isFull) flush
  }

  /**
    * 处理`MysqlInOrderSinkerCheckBatch`事件，当上次发送距离现在超过了20ms才会刷新
    *
    * @return 刷新是否成功
    */
  private def handleCheckFlush: Try[Unit] = Try {
    lazy val now = System.currentTimeMillis()
    if (System.currentTimeMillis() - lastFlushTime >= SettingConstant.SINKER_FLUSH_INTERVAL) {
      flush
      lastFlushTime = now
    }
  }

  /**
    * 刷新方法
    *
    * 将RingBuffer里所有的元素都形成sql 以batch的形式更新到数据库中
    */
  private def flush: Unit = {

    if (!ringBuffer.isEmpty) {
      lastBinlogPosition = Option(ringBuffer.peek).map(_.binlogPositionInfo)
      lazy val connection = sinkFunc.getJdbcConnection
      val startTime = System.currentTimeMillis()
      val elemNum = ringBuffer.elemNum
      try {
        connection.setAutoCommit(false)
        val statement = connection.createStatement()
        ringBuffer.foreach {
          x =>
            if (x.sql.nonEmpty) x.sql.foreach(statement.addBatch(_))
        }
        statement.executeBatch()
        connection.commit()
        statement.clearBatch()
      } catch {
        case e => throw e
      } finally {
        Try(connection.close())
      }
      sendCost((System.currentTimeMillis() - startTime) / elemNum)
      sendCount(elemNum)
    }
  }

  /**
    * 错位次数阈值
    */
  override val errorCountThreshold: Int = 0 //意味着必然不重试

  /**
    * 错位次数
    */
  override var errorCount: Int = 0

  /**
    * 错误处理
    *
    */
  override final def processError(e: Throwable, message: WorkerMessage): Unit = {
    e.printStackTrace()
    errorCount = errorCount + 1
    if (errorCount > errorCountThreshold) {

      lazy val positionInfo = Try(message.msg.asInstanceOf[MysqlRowDataInfo].binlogPositionInfo) match {
        case Success(x) => Option(x)
        case _ => lastBinlogPosition
      }
      positionRecorder.fold(log.error(s"cannot find positionRecorder when sinker$num throw error,id:$syncTaskId")) { ref => ref ! SinkerMessage(MysqlInOrderSinkerGetAbnormal(e, positionInfo)) }
      context.become(error, true)
    }
  }

  override def preStart(): Unit = {
    super.preStart()
    context.system.scheduler.schedule(SettingConstant.SINKER_FLUSH_INTERVAL milliseconds, SettingConstant.SINKER_FLUSH_INTERVAL milliseconds, self, BatcherMessage(MysqlInOrderSinkerCheckBatch))
  }
}

object MysqlBinlogInOrderMysqlRingBufferSinker {
  val name = MysqlBinlogInOrderMysqlRingBufferSinker.getClass.getName.stripSuffix("$")

  def props(taskManager: MysqlSinkManagerImp with TaskManager, num: Int): Props = Props(new MysqlBinlogInOrderMysqlRingBufferSinker(taskManager, num))
}



