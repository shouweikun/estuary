package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, SinkerMessage, WorkerMessage}
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.util.SimpleEstuaryRingBuffer
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinkerCommand.MysqlInOrderSinkerCheckBatch
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{BinlogPositionInfo, MysqlRowDataInfo}
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp

import scala.collection.mutable.ListBuffer
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
    * ringBuffer
    */
  protected lazy val ringBuffer = new SimpleEstuaryRingBuffer[MysqlRowDataInfo](255) //支持batch
  /**
    * SimpleSinker
    */
  lazy val realSinker = context.child("sinker")
  /**
    * 上一次的Binlog位点
    */
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
      val count = ringBuffer.elemNum
      val listBuffer = new ListBuffer[String]
      ringBuffer.foreach(x => if (x.sql.nonEmpty) x.sql.foreach(listBuffer.append(_)))
      realSinker.map(ref => ref ! SinkerMessage(SqlList(listBuffer.toList, lastBinlogPosition, count)))
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
  override final def processError(e: Throwable, message: WorkerMessage): Unit = {}

  /**
    * 初始化Sinker
    */
  private def initSinker: Unit = {
    context.actorOf(SimpleSinker.props(taskManager, num).withDispatcher("akka.sinker-dispatcher"), "sinker")
  }

  override def preStart(): Unit = {
    super.preStart()
    initSinker
    context.system.scheduler.schedule(SettingConstant.SINKER_FLUSH_INTERVAL milliseconds, SettingConstant.SINKER_FLUSH_INTERVAL milliseconds, self, BatcherMessage(MysqlInOrderSinkerCheckBatch))
  }
}

object MysqlBinlogInOrderMysqlRingBufferSinker {
  val name = MysqlBinlogInOrderMysqlRingBufferSinker.getClass.getName.stripSuffix("$")

  def props(taskManager: MysqlSinkManagerImp with TaskManager, num: Int): Props = Props(new MysqlBinlogInOrderMysqlRingBufferSinker(taskManager, num))
}



