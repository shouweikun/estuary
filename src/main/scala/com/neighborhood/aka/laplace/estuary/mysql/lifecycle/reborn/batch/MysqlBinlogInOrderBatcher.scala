package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.bean.exception.control.WorkerCannotFindException
import com.neighborhood.aka.laplace.estuary.bean.exception.other.WorkerInitialFailureException
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.BatcherMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataBatcherPrototype
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.EntryKeyClassifier
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.adapt.MysqlBinlogInOrderPowerAdapterCommand.MysqlBinlogInOrderPowerAdapterUpdateCost
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp.MysqlBinlogInOrderMysqlBatcher
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.count.MysqlBinlogInOrderProcessingCounterCommand.MysqlBinlogInOrderProcessingCounterUpdateCount
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp

/**
  * Created by john_liu on 2018/5/8.
  * MysqlSource的Batcher的原型
  *
  * @tparam R batcher产生的结果类型
  * @author neighborhood.aka.laplace
  * @note 添加新的batcher， 一定要在工厂方法中增加
  */
abstract class MysqlBinlogInOrderBatcher[R](
                                             override val taskManager: MysqlSourceManagerImp with TaskManager,
                                             override val sinker: ActorRef,
                                             override val num: Int = -1
                                           ) extends SourceDataBatcherPrototype[EntryKeyClassifier, R] {


  /**
    * 同步任务id
    */
  def syncTaskId: String = syncTaskId_

  /**
    * 计数器
    */
  def processingCounter: Option[ActorRef] = processingCounter_

  /**
    * 功率控制器
    */
  def powerAdapter: Option[ActorRef] = powerAdapter_

  /**
    * 是否计数
    */
  def isCounting = isCounting_

  /**
    * 是否计时
    */
  def isCosting = isCosting_


  private lazy val powerAdapter_ : Option[ActorRef] = taskManager.powerAdapter
  private lazy val syncTaskId_ : String = taskManager.syncTaskId
  private lazy val processingCounter_ : Option[ActorRef] = taskManager.processingCounter
  private lazy val isCounting_ = taskManager.isCounting
  private lazy val isCosting_ = taskManager.isCosting

  override def receive: Receive = {

    case x: EntryKeyClassifier => transferEntryAndSend(x)
    case x => log.warning(s" batcher unhandled message，$x,id:$syncTaskId")

  }

  /**
    * 处理entry并发送
    *
    * @param x
    */
  protected def transferEntryAndSend(x: EntryKeyClassifier) = {
    val before = System.currentTimeMillis()
    lazy val result = transform(x)
    log.debug(s"batch primaryKey:${
      x.consistentHashKey
    },eventType:${
      x.entry.getHeader.getEventType
    },id:$syncTaskId")
    send2Sinker(BatcherMessage(result))
    //性能分析
    sendCostingMessage(System.currentTimeMillis() - before)
    sendCountingMessage(1)
  }

  @inline
  protected def send2Sinker(message: => Any) = sinker ! message

  @inline
  protected def sendCostingMessage(cost: Long) = if (isCosting) powerAdapter.fold(throw new WorkerCannotFindException(s"cannot find powerAdapter,id:$syncTaskId")
  )(ref => ref ! BatcherMessage(MysqlBinlogInOrderPowerAdapterUpdateCost(cost)))

  @inline
  protected def sendCountingMessage(count: Int = 1) = if (isCounting) processingCounter.fold(throw new WorkerCannotFindException(s"cannot find processCounter,id:$syncTaskId"))(ref => ref ! BatcherMessage(MysqlBinlogInOrderProcessingCounterUpdateCount(count)))

  /**
    * 错位次数阈值
    */
  override def errorCountThreshold: Int = 0

  /**
    * 错位次数
    */
  override var errorCount: Int = _

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???

  override def preStart(): Unit = {
    log.debug(s"init batcher$num,id:$syncTaskId")

  }

  override def postStop(): Unit = {

  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.debug(s"batcher$num process preRestart,id:$syncTaskId")
    super.preRestart(reason: Throwable, message: Option[Any])

  }

  override def postRestart(reason: Throwable): Unit = {
    log.debug(s"batcher$num process postRestart,id:$syncTaskId")

  }
}

object MysqlBinlogInOrderBatcher {
  /**
    * 生产batcher的工厂方法
    *
    * @param name        名称
    * @param taskManager 任务管理器
    * @param sinker      sinker的引用
    * @param num         编号
    * @return 构建好的Props
    */
  def buildMysqlBinlogInOrderBatcher(
                                      name: String,
                                      taskManager: MysqlSourceManagerImp with TaskManager,
                                      sinker: ActorRef,
                                      num: Int = -1): Props = {
    name match {
      case MysqlBinlogInOrderMysqlBatcher.name => MysqlBinlogInOrderMysqlBatcher.props(taskManager, sinker, num)
      case _ => throw new WorkerInitialFailureException(s"cannot build MysqlBinlogInOrderBatcher name item match $name")
    }
  }
}
