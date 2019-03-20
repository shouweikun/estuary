package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.hbase

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.bean.support.HBasePut
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, SinkerMessage}
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerPrototype
import com.neighborhood.aka.laplace.estuary.core.sink.hbase.{HBaseSinkFunc, HBaseSinkManager}
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt.OplogPowerAdapterCommand.OplogPowerAdapterUpdateCost
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.count.OplogProcessingCounterCommand.OplogProcessingCounterUpdateCount
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset

import scala.util.Try

/**
  * Created by john_liu on 2019/3/14.
  *
  * @author neighborhood.aka.laplace
  */
private[hbase] class SimpleHBasePutSinker(
                                           val taskManager: HBaseSinkManager with TaskManager,
                                           override val num: Int
                                         ) extends SourceDataSinkerPrototype[HBaseSinkFunc, HBasePut[MongoOffset]] {


  /**
    * 资源管理器
    */
  override val sinkManger: SinkManager[HBaseSinkFunc] = taskManager

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

  /**
    * sinkFunc
    */
  override val sinkFunc: HBaseSinkFunc = taskManager.sink


  override val isCounting = taskManager.isCounting
  override val isCosting = taskManager.isCosting

  override lazy val positionRecorder = taskManager.positionRecorder
  override lazy val processingCounter = taskManager.processingCounter
  override lazy val powerAdapter = taskManager.powerAdapter

  override def receive: Receive = {
    case x: HBasePut[MongoOffset] => handleSinkTask(x).failed.foreach(e => processError(e, SinkerMessage(x)))
    case m@BatcherMessage(x: HBasePut[MongoOffset]) => handleSinkTask(x).failed.foreach(e => processError(e, m))
    case m@SinkerMessage(x: HBasePut[MongoOffset]) => handleSinkTask(x).failed.foreach(e => processError(e, m))

  }

  /**
    * 处理Batcher转换过的数据
    *
    * @param input batcher转换完的数据
    * @tparam I 类型参数 逆变
    */
  override protected def handleSinkTask[I <: HBasePut[MongoOffset]](input: I): Try[_] = Try {
    lazy val hTable = sink.getTable(input.tableName)
    if (!input.isAbnormal) {
      hTable.setAutoFlush(true, true)
      hTable.put(input.put)
      sendCost(System.currentTimeMillis() - input.ts)
    }
    sendCount(1)
    hTable.close()
  }

  /**
    * 处理批量Batcher转换过的数据
    *
    * @param input batcher转换完的数据集合
    * @tparam I 类型参数 逆变
    */
  override protected def handleBatchSinkTask[I <: HBasePut[MongoOffset]](input: List[I]): Try[_] = {
    ???
  }

  /**
    * 发送计数
    *
    * @param count
    */
  override protected def sendCount(count: => Long): Unit = if (isCounting) this.processingCounter.fold(log.warning(s"cannot find processingCounter when send sink Count,id:$syncTaskId"))(ref => ref ! SinkerMessage(OplogProcessingCounterUpdateCount(count)))

  /**
    * 发送耗时
    *
    * @param cost
    */
  override protected def sendCost(cost: => Long): Unit = if (isCosting) this.powerAdapter.fold(log.warning(s"cannot find powerAdapter when sinker sending cost,id:$syncTaskId"))(ref => ref ! SinkerMessage(OplogPowerAdapterUpdateCost(cost)))

  /**
    * 错位次数阈值
    */
  override def errorCountThreshold: Int = 0

  /**
    * 错位次数
    */
  override var errorCount: Int = 0

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = throw e

}

object SimpleHBasePutSinker {
  def props(taskManager: HBaseSinkManager with TaskManager, num: Int): Props = Props(new SimpleHBasePutSinker(taskManager, num))
}
