package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.hbase

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{ActorRef, AllForOneStrategy, Props}
import akka.routing.{ConsistentHashingGroup, RoundRobinGroup}
import com.neighborhood.aka.laplace.estuary.bean.key.{OplogKey, PartitionStrategy}
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataBatcherManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{BatcherMessage, FetcherMessage, SyncControllerMessage}
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.OplogClassifier
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.OplogBatcherCommand.{OplogBatcherCheckHeartbeats, OplogBatcherStart}
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoSourceManagerImp}
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import org.bson.Document

/**
  * Created by john_liu on 2019/3/1.
  *
  * @author neighborhood.aka.laplace
  */
final class OplogHBaseBatcherManager(
                                      override val taskManager: MongoSourceManagerImp with TaskManager,
                                      override val sinker: ActorRef

                                    ) extends SourceDataBatcherManagerPrototype[MongoConnection, KafkaSinkFunc[OplogKey, String]] {

  val partitionStrategy = taskManager.partitionStrategy

  lazy val router = context.child(routerName)

  lazy val specialInfoSender = context.child(specialInfoSenderName)
  /**
    * 事件收集器
    */
  override lazy val eventCollector: Option[ActorRef] = taskManager.eventCollector

  /**
    * 是否是最上层的manager
    */
  override def isHead: Boolean = true

  /**
    * 编号
    */
  override def num: Int = -1

  /**
    * 同步任务id
    */
  override lazy val syncTaskId: String = taskManager.syncTaskId
  /**
    * batcher数量
    */
  val batcherNum = taskManager.batcherNum
  /**
    * batcher组件动态加载
    */
  val batcherNameToLoad = taskManager.batcherNameToLoad

  /**
    * 初始化Batchers
    */
  override protected def initBatchers: Unit = {
    taskManager.wait4SinkerList() //必须要等待,一定要等sinkerList创建完毕才行
    //val batcherTypeName = taskManager.batcherNameToLoad.get(batcherName).getOrElse(OplogKafkaBatcher.name) // todo 支持动态加载
    val paths: List[String] = (1 to batcherNum).map {
      index => OplogHBaseBatcher.props(taskManager, taskManager.sinkerList((index - 1) % (taskManager.sinkerList.size)), index).withDispatcher("akka.batcher-dispatcher")
    }.map(context.actorOf(_)).map(_.path.toString).toList
    lazy val roundRobin = context.actorOf(new RoundRobinGroup(paths).props().withDispatcher("akka.batcher-dispatcher"), "router")
    lazy val consistentHashing = context.actorOf(new ConsistentHashingGroup(paths, virtualNodesFactor = SettingConstant.HASH_MAPPING_VIRTUAL_NODES_FACTOR).props().withDispatcher("akka.batcher-dispatcher"), routerName)
    partitionStrategy match { //暂未支持其他分区等级
      case  _=>   roundRobin //先强行roundRobin
      case PartitionStrategy.PRIMARY_KEY => consistentHashing
      case PartitionStrategy.DATABASE_TABLE => consistentHashing
      case PartitionStrategy.MOD => roundRobin
    }
    //    val specialInfoSenderTypeName = batcherNameToLoad.get(specialInfoSenderName).getOrElse(MysqlBinlogInOrderMysqlSpecialInfoSender.name) //todo 动态加载能力
    val props = OplogHBaseSpecialInfoSender.props(taskManager.sinkerList.head, taskManager)
    context.actorOf(props, specialInfoSenderName)
  }


  override def receive: Receive = {
    case SyncControllerMessage(OplogBatcherStart) => start
  }

  def online: Receive = {
    case doc: Document => dispatchDoc(doc)
    case FetcherMessage(doc: Document) => dispatchDoc(doc)
    case SyncControllerMessage(OplogBatcherCheckHeartbeats) => dispatchHeartbeatMessage
  }

  private def start: Unit = {
    log.info(s"batcher manager switch 2 online,id:$syncTaskId")
    context.become(online, true)
    batcherChangeStatus(Status.ONLINE)
  }

  private def dispatchDoc(doc: Document): Unit = {
    router.fold(log.error(s"router cannot be null when dispatch doc,id:$syncTaskId"))(ref => ref ! OplogClassifier(doc = doc, partitionStrategy = partitionStrategy))
  }

  private def dispatchHeartbeatMessage: Unit = specialInfoSender.fold(log.error(s"cannot find special info sender when dispatch heartbeat,id:$syncTaskId")) {
    ref => ref ! BatcherMessage(OplogBatcherCheckHeartbeats)
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


  /**
    * ********************* 状态变化 *******************
    */
  protected def changeFunc(status: Status)

  = TaskManager.changeFunc(status, taskManager)

  protected def onChangeFunc

  = TaskManager.onChangeStatus(taskManager)

  protected def batcherChangeStatus(status: Status)

  = TaskManager.changeStatus(status, changeFunc, onChangeFunc)


  /**
    * ********************* Actor生命周期 *******************
    */
  override def preStart(): Unit

  = {
    //状态置为offline
    batcherChangeStatus(Status.OFFLINE)
    initBatchers
    log.info(s"switch batcher to offline,id:$syncTaskId")
  }

  override def postStop(): Unit

  = {
    log.info(s"batcher process postStop,id:$syncTaskId")
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit

  = {
    log.info(s"batcher process preRestart,id:$syncTaskId")
    batcherChangeStatus(Status.RESTARTING)
    super.preRestart(reason, message)

  }

  override def postRestart(reason: Throwable): Unit

  = {
    log.info(s"batcher process postRestart,id:$syncTaskId")
    super.postRestart(reason)
  }

  override def supervisorStrategy

  = {
    AllForOneStrategy() {
      case _ => {
        batcherChangeStatus(Status.ERROR)
        Escalate
      }
    }
  }

}

object OplogHBaseBatcherManager {
  def props(taskManager: MongoSourceManagerImp with TaskManager, sinker: ActorRef): Props = Props(new OplogHBaseBatcherManager(taskManager, sinker))
}