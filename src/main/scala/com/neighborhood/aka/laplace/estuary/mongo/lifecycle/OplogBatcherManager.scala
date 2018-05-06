package com.neighborhood.aka.laplace.estuary.mongo.lifecycle

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, ActorRef, AllForOneStrategy}
import akka.routing.{ConsistentHashingGroup, ConsistentHashingPool}
import akka.routing.ConsistentHashingRouter.ConsistentHashable
import com.mongodb.DBObject
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SourceDataBatcher, Status}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mongo.SettingConstant
import com.neighborhood.aka.laplace.estuary.mongo.task.Mongo2KafkaTaskInfoManager

/**
  * Created by john_liu on 2018/5/2.
  */
class OplogBatcherManager(
                           mongo2KafkaTaskInfoManager: Mongo2KafkaTaskInfoManager
                         )
  extends SourceDataBatcher with Actor with ActorLogging {


  val syncTaskId: String = mongo2KafkaTaskInfoManager.syncTaskId
  ConsistentHashingPool
  val batcherNum = mongo2KafkaTaskInfoManager.batcherNum
  //todo
  lazy val router: Option[ActorRef] = context.child("router")


  override def receive: Receive = {
    case _ =>
  }

  def online: Receive = {

    case dbObject: DBObject => {
      import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.OplogBatcherManager._
      router.fold(throw new Exception(s"batcher router should not be none,id:$syncTaskId"))(ref => ref ! Classifer(dbObject))
      log.debug(s"batcherManager processes oplog,_id:${dbObject.get("_id")},id:$syncTaskId")
    }
  }

  private def initBatchersAndRouter: Unit = {
    lazy val paths =
      (1 to batcherNum)
        .map {
          index =>
            context
              .actorOf(OplogBatcher.prop(mongo2KafkaTaskInfoManager, index), s"batcher$index")
              .path
              .toString
        }
    context.actorOf(new ConsistentHashingGroup(paths, virtualNodesFactor = SettingConstant.HASH_MAPPING_VIRTUAL_NODES_FACTOR).props(), "router")
  }

  /**
    * ********************* 状态变化 *******************
    */
  private def changeFunc(status: Status): Unit = TaskManager.changeFunc(status, mongo2KafkaTaskInfoManager)

  private def onChangeFunc: Unit = TaskManager.onChangeStatus(mongo2KafkaTaskInfoManager)

  private def batcherChangeStatus(status: Status): Unit = TaskManager.changeStatus(status, changeFunc, onChangeFunc)

  /**
    * ********************* Actor生命周期 *******************
    */
  override def preStart(): Unit = {

    //状态置为offline
    batcherChangeStatus(Status.OFFLINE)
    initBatchersAndRouter
    log.info(s"batcherManager init batchers and router,id:${syncTaskId}")
  }

  override def postRestart(reason: Throwable): Unit = {
    super.postRestart(reason)
    batcherChangeStatus(Status.RESTARTING)
    log.info(s"fetcher will restart in ${SettingConstant.TASK_RESTART_INTERVAL}s")
  }

  override def postStop(): Unit = {

  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    batcherChangeStatus(Status.ERROR)
    super.preRestart(reason, message)
  }

  override def supervisorStrategy = {
    AllForOneStrategy() {
      case _ => {
        batcherChangeStatus(Status.ERROR)
        Escalate
      }
    }
  }

  /**
    * 错位次数阈值
    */
  override var errorCountThreshold: Int = _
  /**
    * 错位次数
    */
  override var errorCount: Int = _

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???
}

object OplogBatcherManager {

  sealed case class Classifer(doc: DBObject) extends ConsistentHashable {
    override def consistentHashKey: Any = {
      //todo
      s"${doc.get("_id")}"

    }
  }

}