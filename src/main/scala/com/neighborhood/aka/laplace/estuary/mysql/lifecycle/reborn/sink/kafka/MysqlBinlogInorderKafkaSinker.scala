package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.kafka

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.BatcherMessage
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinker
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.MysqlBinlogInOrderSinkerCommand.MysqlInOrderSinkerGetAbnormal

import scala.util.Try

/**
  * Created by john_liu on 2019/2/26.
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInorderKafkaSinker(
                                           override val taskManager: SinkManager[KafkaSinkFunc[BinlogKey, String]] with TaskManager,
                                           override val num: Int = -1
                                         ) extends MysqlBinlogInOrderSinker[KafkaSinkFunc[BinlogKey, String], KafkaMessage](taskManager, num) {

  /**
    * 处理Batcher转换过的数据
    *
    * @param input batcher转换完的数据
    * @tparam I 类型参数 逆变
    */
  override protected def handleSinkTask[I <: KafkaMessage](input: I): Try[Unit] = Try {
    val key = input.baseDataJsonKey.asInstanceOf[BinlogKey]
    lazy val binlogPosition = Try(BinlogPositionInfo(key.getMysqlJournalName, key.getMysqlPosition, key.getMysqlTimestamp)).toOption
    sink.send(key, input.jsonValue, new KafkaSendCallback(positionRecorder, self, binlogPosition))
  }

  /**
    * 处理批量Batcher转换过的数据
    *
    * @param input batcher转换完的数据集合
    * @tparam I 类型参数 逆变
    */
  override protected def handleBatchSinkTask[I <: KafkaMessage](input: List[I]): Try[_] = Try {
    input.map(handleSinkTask(_))
  }

  override def receive: Receive = {
    case BatcherMessage(x: KafkaMessage) => handleSinkTask(x)
    case BatcherMessage(x: List[_]) if (x.head.isInstanceOf[KafkaMessage]) => handleBatchSinkTask(x)
    case BatcherMessage(x: MysqlInOrderSinkerGetAbnormal) => context.become(error, true)

  }

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

object MysqlBinlogInorderKafkaSinker {
  val name = MysqlBinlogInorderKafkaSinker.getClass.getName.stripSuffix("$")

  def props(taskManager: SinkManager[KafkaSinkFunc[BinlogKey, String]] with TaskManager, num: Int = -1): Props = Props(new MysqlBinlogInorderKafkaSinker(taskManager, num))
}