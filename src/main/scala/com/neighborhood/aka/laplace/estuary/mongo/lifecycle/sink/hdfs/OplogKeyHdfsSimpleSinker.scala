package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.hdfs


import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.bean.support.HdfsMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, SinkerMessage}
import com.neighborhood.aka.laplace.estuary.core.sink.hdfs.HdfsSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt.OplogPowerAdapterCommand.OplogPowerAdapterUpdateCost
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.count.OplogProcessingCounterCommand.OplogProcessingCounterUpdateCount
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.OplogSinkerCommand.OplogSinkerCollectOffset
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.OplogSinkerEvent.OplogSinkerOffsetCollected
import com.neighborhood.aka.laplace.estuary.mongo.sink.hdfs.HdfsSinkManagerImp
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset
import org.apache.hadoop.fs.FSDataOutputStream

import scala.util.Try

/**
  * Created by john_liu on 2019/3/4.
  *
  * @author neighborhood.aka.laplace
  */
final class OplogKeyHdfsSimpleSinker(
                                      override val taskManager: HdfsSinkManagerImp with TaskManager,
                                      override val num: Int
                                    ) extends SourceDataSinkerPrototype[HdfsSinkFunc, HdfsMessage[MongoOffset]] {
  /**
    * 资源管理器
    */
  override val sinkManger: HdfsSinkManagerImp = taskManager

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

  /**
    * sinkFunc
    */
  override lazy val sinkFunc: HdfsSinkFunc = sinkManger.sink


  override lazy val processingCounter = taskManager.processingCounter

  override lazy val powerAdapter = taskManager.powerAdapter

  override lazy val positionRecorder = taskManager.positionRecorder

  lazy val sinkAbnormal = taskManager.sinkAbnormal

  private var lastOffset: Option[MongoOffset] = None

  private var fsStream: Option[FSDataOutputStream] = None


  /**
    * 处理Batcher转换过的数据
    *
    * @param input batcher转换完的数据
    * @tparam I 类型参数 逆变
    */
  override protected def handleSinkTask[I <: HdfsMessage[MongoOffset]](input: I): Try[_] = Try {
    if (!input.isAbnormal) {
      if (fsStream.isEmpty) fsStream = Option(sink.getOutputStream(input.dbName, input.tableName, input.offset.mongoTsSecond))
      fsStream.foreach(s => sink.send(input, s))
      sendCost(System.currentTimeMillis() - input.ts)
      lastOffset = Option(input.offset)
    }
    sendCount(1)
  }

  /**
    * 处理批量Batcher转换过的数据
    *
    * @param input batcher转换完的数据集合
    * @tparam I 类型参数 逆变
    */
  override protected def handleBatchSinkTask[I <: HdfsMessage[MongoOffset]](input: List[I]): Try[_] = Try {
    input.foreach(in => handleSinkTask(in))
  }

  override def receive: Receive = {
    case BatcherMessage(hdfsMessage: HdfsMessage[MongoOffset]) => handleSinkTask(hdfsMessage)
    case hdfsMessage: HdfsMessage[MongoOffset] => handleSinkTask(hdfsMessage)
    case SinkerMessage(OplogSinkerCollectOffset) => lastOffset.map(x => sender() ! OplogSinkerOffsetCollected(x))
    case OplogSinkerCollectOffset => lastOffset.map(x => sender() ! OplogSinkerOffsetCollected(x))
  }


  override protected def sendCost(cost: => Long): Unit = powerAdapter.map(ref => ref ! SinkerMessage(OplogPowerAdapterUpdateCost(cost)))

  override protected def sendCount(count: => Long): Unit = processingCounter.map(ref => ref ! SinkerMessage(OplogProcessingCounterUpdateCount(count: Long)))

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
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???
}

object OplogKeyHdfsSimpleSinker {
  def props(taskManager: HdfsSinkManagerImp with TaskManager, num: Int): Props = Props(new OplogKeyHdfsSimpleSinker(taskManager, num))
}
