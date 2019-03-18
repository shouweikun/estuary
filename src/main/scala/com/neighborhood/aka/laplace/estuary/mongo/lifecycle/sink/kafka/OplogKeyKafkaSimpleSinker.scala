package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.kafka

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.bean.key.OplogKey
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, SinkerMessage}
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt.OplogPowerAdapterCommand.OplogPowerAdapterUpdateCost
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.count.OplogProcessingCounterCommand.OplogProcessingCounterUpdateCount
import com.neighborhood.aka.laplace.estuary.mongo.sink.kafka.OplogKeyKafkaSinkManagerImp
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset

import scala.util.Try

/**
  * Created by john_liu on 2019/3/4.
  *
  * @author neighborhood.aka.laplace
  */
final class OplogKeyKafkaSimpleSinker(
                                       override val taskManager: OplogKeyKafkaSinkManagerImp with TaskManager,
                                       override val num: Int
                                     ) extends SourceDataSinkerPrototype[KafkaSinkFunc[OplogKey, String], KafkaMessage] {
  /**
    * 资源管理器
    */
  override val sinkManger: OplogKeyKafkaSinkManagerImp = taskManager

  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId

  /**
    * sinkFunc
    */
  override lazy val sinkFunc: KafkaSinkFunc[OplogKey, String] = sinkManger.sink


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
  override protected def handleSinkTask[I <: KafkaMessage](input: I): Try[_] = Try {
    if (!input.isAbnormal) {
      val key = input.baseDataJsonKey.asInstanceOf[OplogKey]
      val mongoOffset = MongoOffset(key.getMongoTsSecond, key.getMongoTsInc)
      sink.send(key, input.jsonValue, new OplogSinkCallback(sinkAbnormal, positionRecorder.get, mongoOffset))
      positionRecorder.map(ref => ref ! mongoOffset) //发送offset
      sendCost(System.currentTimeMillis() - input.baseDataJsonKey.msgSyncStartTime)
    }
    sendCount(1)
  }

  /**
    * 处理批量Batcher转换过的数据
    *
    * @param input batcher转换完的数据集合
    * @tparam I 类型参数 逆变
    */
  override protected def handleBatchSinkTask[I <: KafkaMessage](input: List[I]): Try[_] = Try {
    input.foreach(in => handleSinkTask(in))
  }

  override def receive: Receive = {
    case BatcherMessage(kafkaMessage: KafkaMessage) => handleSinkTask(kafkaMessage)
    case kafkaMessage: KafkaMessage => handleSinkTask(kafkaMessage)
  }


  def sendCost(cost: Long) = powerAdapter.map(ref => ref ! SinkerMessage(OplogPowerAdapterUpdateCost(cost)))

  def sendCount(count: Long) = processingCounter.map(ref => ref ! SinkerMessage(OplogProcessingCounterUpdateCount(count: Long)))

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

object OplogKeyKafkaSimpleSinker {
  def props(taskManager: OplogKeyKafkaSinkManagerImp with TaskManager, num: Int): Props = Props(new OplogKeyKafkaSimpleSinker(taskManager, num))
}