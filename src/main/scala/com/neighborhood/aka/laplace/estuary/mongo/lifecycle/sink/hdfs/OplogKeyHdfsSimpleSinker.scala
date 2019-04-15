package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.hdfs


import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.bean.key.OplogKey
import com.neighborhood.aka.laplace.estuary.bean.support.{HdfsMessage, KafkaMessage}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, SinkerMessage}
import com.neighborhood.aka.laplace.estuary.core.sink.hdfs.HdfsSinkFunc
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt.OplogPowerAdapterCommand.OplogPowerAdapterUpdateCost
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.count.OplogProcessingCounterCommand.OplogProcessingCounterUpdateCount
import com.neighborhood.aka.laplace.estuary.mongo.sink.hdfs.{HdfsSinkImp, HdfsSinkManagerImp}
import com.neighborhood.aka.laplace.estuary.mongo.sink.kafka.OplogKeyKafkaSinkManagerImp
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset

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

  /**
    * 处理Batcher转换过的数据
    *
    * @param input batcher转换完的数据
    * @tparam I 类型参数 逆变
    */
  override protected def handleSinkTask[I <: HdfsMessage[MongoOffset]](input: I): Try[_] = Try {
    if (!input.isAbnormal) {
      sink.send(input)
      positionRecorder.map(ref => ref ! input.offset) //发送offset
      sendCost(System.currentTimeMillis() - input.offset.mongoTsSecond*1000)//TODO 需要一个从mongo获取到数据到时间
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
