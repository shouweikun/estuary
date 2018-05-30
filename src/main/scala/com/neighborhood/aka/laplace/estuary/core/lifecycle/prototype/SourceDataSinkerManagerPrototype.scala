package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.{SourceDataSinker, Status}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2018/5/21.
  */
trait SourceDataSinkerManagerPrototype extends ActorPrototype with SourceDataSinker {
  /**
    * 是否是顶部
    */
  val isHead:Boolean
  /**
    * 任务信息管理器
    */
  val taskManager: TaskManager

  /**
    * 同步任务id
    */
  override val syncTaskId = taskManager.syncTaskId


  protected def changeFunc(status: Status) = TaskManager.changeFunc(status, taskManager)

  protected def onChangeFunc = TaskManager.onChangeStatus(taskManager)

  protected def sinkerChangeStatus(status: Status) = TaskManager.changeStatus(status, changeFunc, onChangeFunc)

  /**
    * **************** Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    sinkerChangeStatus(Status.OFFLINE)


  }

  override def postStop(): Unit = {
    log.info(s"sinker processing postStop,id:$syncTaskId")

    //    kafkaSinker.kafkaProducer.close()
    //    sinkTaskPool.environment.shutdown()
    //logPositionHandler.logPositionManage
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"sinker processing preRestart,id:$syncTaskId")
    sinkerChangeStatus(Status.ERROR)
    context.become(receive)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"sinker processing preRestart,id:$syncTaskId")
    sinkerChangeStatus(Status.RESTARTING)
    super.postRestart(reason)
  }
}
