package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.SourceDataBatcher
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat

/**
  * Created by john_liu on 2018/5/20.
  */
trait SourceDataBatcherPrototype[A, B] extends ActorPrototype with SourceDataBatcher {


  def mappingFormat: MappingFormat[A, B]

  /**
    * sinker 的ActorRef
    */
  def sinker: ActorRef

  /**
    * 编号
    */
  def num: Int

  /**
    * 核心转换方法
    *
    * @param a 待转换的
    * @return 转换结果
    */
  def transform(a: A): B = mappingFormat.transform(a) // todo 这么干有问题

  override def preStart(): Unit = {
    log.debug(s"init batcher$num,id:$syncTaskId")
  }

  override def postStop(): Unit = {

  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"batcher$num process preRestart,id:$syncTaskId")
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"batcher$num process postRestart,id:$syncTaskId")
    super.postRestart(reason)
  }
}
