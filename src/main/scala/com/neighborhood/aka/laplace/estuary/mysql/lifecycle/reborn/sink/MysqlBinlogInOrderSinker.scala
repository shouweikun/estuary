package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{OneForOneStrategy, Props}
import com.neighborhood.aka.laplace.estuary.bean.exception.other.WorkerInitialFailureException
import com.neighborhood.aka.laplace.estuary.core.lifecycle.SinkerMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerPrototype
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.adapt.MysqlBinlogInOrderPowerAdapterCommand.MysqlBinlogInOrderPowerAdapterUpdateCost
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.count.MysqlBinlogInOrderProcessingCounterCommand.MysqlBinlogInOrderProcessingCounterUpdateCount
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp

import scala.reflect.ClassTag

/**
  * Created by john_liu on 2018/5/8.
  *
  * @tparam B SinkFunc
  * @tparam R Batch 生成的Result
  * @author neighborhood.aka.laplace
  * @note 在工厂方法中增加
  */
abstract class MysqlBinlogInOrderSinker[B <: SinkFunc, R: ClassTag](
                                                                     override val taskManager: SinkManager[B] with TaskManager,
                                                                     override val num: Int = -1
                                                                   ) extends SourceDataSinkerPrototype[B, R] {


  /**
    * 资源管理器
    */
  override val sinkManger: SinkManager[B] = taskManager
  /**
    * 同步任务id
    */
  override val syncTaskId = taskManager.syncTaskId
  /**
    * sink
    */
  lazy val sinkFunc: B = taskManager.sink
  /**
    * 是否同步写
    */
  val isSyncWrite = taskManager.isSync
  /**
    * 功率控制器
    */
  lazy val powerAdapter = taskManager.powerAdapter
  /**
    * 计数器
    */
  lazy val processingCounter = taskManager.processingCounter
  /**
    * 是否计数
    */
  val isCounting = taskManager.isCounting
  /**
    * 是否计算耗时
    */
  val isCosting = taskManager.isCounting

  /**
    * 出错，挂起
    *
    * @return
    */
  protected def error: Receive = {
    //    case x => log.warning(s"since sinker get abnormal,$x will not be processed,id:$syncTaskId")
    case _ =>
  }

  /**
    * 发送计数
    *
    * @param count
    */
  protected def sendCount(count: => Long): Unit = if (isCounting) this.processingCounter.fold(log.warning(s"cannot find processingCounter when send sink Count,id:$syncTaskId"))(ref => ref ! SinkerMessage(MysqlBinlogInOrderProcessingCounterUpdateCount(count)))

  /**
    * 发送耗时
    *
    * @param cost
    */
  protected def sendCost(cost: => Long): Unit = if (isCosting) this.powerAdapter.fold(log.warning(s"cannot find powerAdapter when sinker sending cost,id:$syncTaskId"))(ref => ref ! SinkerMessage(MysqlBinlogInOrderPowerAdapterUpdateCost(cost)))


  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: Exception => {
        log.error(s"sinker crashed,exception:$e,cause:${e.getCause},processing SupervisorStrategy,id:$syncTaskId")
        Escalate
      }
      case error: Error => {
        log.error(s"sinker crashed,error:$error,cause:${error.getCause},processing SupervisorStrategy,id:$syncTaskId")
        Escalate
      }
      case e => {
        log.error(s"sinker crashed,throwable:$e,cause:${e.getCause},processing SupervisorStrategy,id:$syncTaskId")
        Escalate
      }
    }
  }
}

object MysqlBinlogInOrderSinker {

  /**
    * 构建Sinker的工厂方法
    *
    * @param name 名称
    * @param taskManager
    * @param num  编号
    * @return 构建好的Props
    */
  def buildMysqlBinlogInOrderSinker(name: String, taskManager: SinkManager[_] with TaskManager, num: Int = -1): Props = {
    val a = MysqlBinlogInOrderMysqlRingBufferSinker.name
    name match {
      case MysqlBinlogInOrderMysqlSinker.name => MysqlBinlogInOrderMysqlSinker.props(taskManager.asInstanceOf[MysqlSinkManagerImp with TaskManager], num)
      case MysqlBinlogInOrderMysqlRingBufferSinker.name => MysqlBinlogInOrderMysqlRingBufferSinker.props(taskManager.asInstanceOf[MysqlSinkManagerImp with TaskManager], num)
      case _ => throw new WorkerInitialFailureException(s"cannot build MysqlBinlogInOrderSinker name item match $name")
    }
  }
}