package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.hbase

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.bean.support.HBasePut
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerPrototype
import com.neighborhood.aka.laplace.estuary.core.sink.hbase.{HBaseSinkFunc, HBaseSinkManager}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.util.SimpleEstuaryRingBuffer
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset
import org.apache.hadoop.hbase.client.Put

import scala.util.Try
import OplogKeyHBaseRingBufferSinker._
import com.neighborhood.aka.laplace.estuary.core.lifecycle.SinkerMessage
import com.neighborhood.aka.laplace.estuary.mongo.SettingConstant
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.OplogSinkerCommand.OplogSinkerCheckBatch

/**
  * Created by john_liu on 2019/3/14.
  *
  * @author neighborhood.aka.laplace
  */
class OplogKeyHBaseRingBufferSinker(
                                     override val taskManager: HBaseSinkManager with TaskManager,
                                     override val num: Int
                                   ) extends SourceDataSinkerPrototype[HBaseSinkFunc, HBasePut[MongoOffset]] {

  private val ringBuffer = new SimpleEstuaryRingBuffer[HBasePut[MongoOffset]](15)
  /**
    * 资源管理器
    */
  override val sinkManger: HBaseSinkManager = taskManager

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

  /**
    * sinkFunc
    */
  override val sinkFunc: HBaseSinkFunc = sinkManger.sink


  lazy val realSinker: ActorRef = context.child(realSinkerName).get

  private var lastSendTimestamp: Long = System.currentTimeMillis()

  override def receive: Receive = {
    case x: HBasePut[MongoOffset] => handleSinkTask(x)
    case SinkerMessage(x: HBasePut[MongoOffset]) => handleSinkTask(x)
    case OplogSinkerCheckBatch => handleCheckBatchTask
    case SinkerMessage(OplogSinkerCheckBatch) => handleCheckBatchTask
  }

  /**
    * 处理Batcher转换过的数据
    *
    * @param input batcher转换完的数据
    * @tparam I 类型参数 逆变
    */
  override protected def handleSinkTask[I <: HBasePut[MongoOffset]](input: I): Try[_] = Try {
    putAndFlushWhenFull(input)
  }

  /**
    * 处理批量Batcher转换过的数据
    *
    * @param input batcher转换完的数据集合
    * @tparam I 类型参数 逆变
    */
  override protected def handleBatchSinkTask[I <: HBasePut[MongoOffset]](input: List[I]): Try[_] = ???


  /**
    * 向ringBuffer中添加元素，满了会触发flush
    *
    * @param x 待添加的元素
    * @return 刷新是否成功
    */
  private def putAndFlushWhenFull(x: HBasePut[MongoOffset]): Try[Unit] = Try {
    if (ringBuffer.isFull) flush()
    ringBuffer.put(x)
    if (ringBuffer.isFull) flush()
  }

  private def handleCheckBatchTask: Unit = {
    val ts = System.currentTimeMillis()
    if (ts - lastSendTimestamp > SettingConstant.SINKER_FLUSH_INTERVAL) {
      flush(ts)
    }
  }

  /**
    * 刷新方法
    *
    * 将RingBuffer里所有的元素都形成sql 以batch的形式更新到数据库中
    */
  private def flush(ts: Long = System.currentTimeMillis()): Unit = {
    if (!ringBuffer.isEmpty) {
      val last = ringBuffer.last
      val offset = last.offset
      val tableName = last.tableName
      val count = ringBuffer.elemNum
      val list = new java.util.LinkedList[Put]()
      ringBuffer.foreach(x => if (!x.isAbnormal) list.add(x.put))
      realSinker ! SinkHolder(tableName, offset, list, count, ts)
      lastSendTimestamp = ts
    }
  }

  private def initRealSinker: Unit = {
    context.actorOf(SimpleSinkHolderSinker.props(taskManager, num).withDispatcher("akka.sinker-dispatcher"))
  }

  override def preStart(): Unit = {
    super.preStart()
    initRealSinker
    self ! OplogSinkerCheckBatch
  }


  /**
    * 错位次数阈值
    */
  override def errorCountThreshold: Int = ???

  /**
    * 错位次数
    */
  override var errorCount: Int = _

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {}


}

object OplogKeyHBaseRingBufferSinker {
  final private[OplogKeyHBaseRingBufferSinker] val realSinkerName = "realSinkerName"

  def props(taskManager: HBaseSinkManager with TaskManager, num: Int): Props = Props(new OplogKeyHBaseRingBufferSinker(taskManager, num))
}
