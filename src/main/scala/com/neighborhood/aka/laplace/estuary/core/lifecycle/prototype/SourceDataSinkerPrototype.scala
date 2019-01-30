package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.SourceDataSinker
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, TaskManager}

import scala.util.Try

/**
  * Created by john_liu on 2018/5/21.
  *
  * @tparam S SinkFunc
  * @tparam R batcher生成的Result
  * @author neighborhood.aka.laplace
  */
trait SourceDataSinkerPrototype[S <: SinkFunc, -R] extends ActorPrototype with SourceDataSinker {
  /**
    * 任务信息管理器
    */
  def taskManager: TaskManager

  /**
    * 资源管理器
    */
  def sinkManger: SinkManager[S]

  /**
    * sink
    */
  protected lazy val sink = sinkManger.sink

  /**
    * 同步任务id
    */
  def syncTaskId: String

  /**
    * sinkFunc
    */
  def sinkFunc: S

  /**
    * 编号
    *
    */
  def num: Int

  /**
    * 处理Batcher转换过的数据
    *
    * @param input batcher转换完的数据
    * @tparam I 类型参数 逆变
    */
  protected def handleSinkTask[I <: R](input: I): Try[_]

  /**
    * 处理批量Batcher转换过的数据
    *
    * @param input batcher转换完的数据集合
    * @tparam I 类型参数 逆变
    */
  protected def handleBatchSinkTask[I <: R](input: List[I]): Try[_]


  override def preStart(): Unit = {

    log.debug(s"init sinker$num,id:$syncTaskId")
    if (sinkFunc.isTerminated) sinkFunc.start
  }

  override def postStop(): Unit = {
    log.debug(s"sinker$num processing postStop,id:$syncTaskId")
    if (sinkFunc.isTerminated) sinkFunc.close
    //    sinkTaskPool.environment.shutdown()
    //logPositionHandler.logPositionManage
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.debug(s"sinker$num processing preRestart,id:$syncTaskId")
  }

  override def postRestart(reason: Throwable): Unit = {
    log.debug(s"sinker$num processing preRestart,id:$syncTaskId")
    super.postRestart(reason)
  }

}


