package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink

import com.neighborhood.aka.laplace.estuary.bean.key.OplogKey
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerPrototype
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mongo.sink.OplogKeyKafkaSinkManagerImp

import scala.util.Try

/**
  * Created by john_liu on 2019/3/4.
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
  override val sinkFunc: KafkaSinkFunc[OplogKey, String] = ???

  /**
    * 处理Batcher转换过的数据
    *
    * @param input batcher转换完的数据
    * @tparam I 类型参数 逆变
    */
  override protected def handleSinkTask[I <: KafkaMessage](input: I): Try[_] = ???

  /**
    * 处理批量Batcher转换过的数据
    *
    * @param input batcher转换完的数据集合
    * @tparam I 类型参数 逆变
    */
  override protected def handleBatchSinkTask[I <: KafkaMessage](input: List[I]): Try[_] = ???

  override def receive: Receive = ???

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
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???
}
