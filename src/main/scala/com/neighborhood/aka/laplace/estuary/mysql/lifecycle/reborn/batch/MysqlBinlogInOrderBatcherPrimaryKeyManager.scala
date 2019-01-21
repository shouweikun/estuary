package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{ActorRef, AllForOneStrategy, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.bean.exception.control.WorkerCannotFindException
import com.neighborhood.aka.laplace.estuary.bean.exception.other.WorkerInitialFailureException
import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataBatcherManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp.MysqlBinlogInOrderMysqlPrimaryKeyManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{DatabaseAndTableNameClassifier, EntryKeyClassifier}
import com.neighborhood.aka.laplace.estuary.mysql.source.{MysqlConnection, MysqlSourceManagerImp}
import com.neighborhood.aka.laplace.estuary.mysql.utils.CanalEntryTransUtil

/**
  * Created by john_liu on 2018/5/8.
  * primaryKeyManager 的抽象
  *
  * @tparam B SinkFunc
  * @author neighborhood.aka.laplace
  */
abstract class MysqlBinlogInOrderBatcherPrimaryKeyManager[B <: SinkFunc](
                                                                          /**
                                                                            * 任务信息管理器
                                                                            */
                                                                          override val taskManager: MysqlSourceManagerImp with TaskManager,

                                                                          /**
                                                                            * sinker 的ActorRef
                                                                            */
                                                                          override val sinker: ActorRef,

                                                                          /**
                                                                            * 编号
                                                                            */
                                                                          override val num: Int
                                                                        ) extends SourceDataBatcherManagerPrototype[MysqlConnection, B] {


  /**
    * 事件收集器
    */
  override val eventCollector: Option[ActorRef] = taskManager.eventCollector
  /**
    * 是否是最上层的manager
    */
  override val isHead: Boolean = false
  /**
    * 同步任务id
    */
  override val syncTaskId: String = taskManager.syncTaskId
  /**
    * 是否同步写
    */
  val partitionStrategy: PartitionStrategy = taskManager.partitionStrategy
  /**
    * batcher的数量
    */
  val batcherNum: Int = taskManager.batcherNum
  /**
    * router的ActorRef
    */
  lazy val router: Option[ActorRef] = context.child(routerName)

  override def receive: Receive = {
    case DatabaseAndTableNameClassifier(entry) => parseValueAndDispatchEntry(entry)
  }

  /**
    * 解析value 并发送entry
    *
    * @param entry             待发送的entry
    * @param partitionStrategy 分区策略
    */
  protected def parseValueAndDispatchEntry(entry: => CanalEntry.Entry, partitionStrategy: PartitionStrategy = this.partitionStrategy): Unit = {
    import scala.collection.JavaConverters._
    lazy val rowChange = CanalEntryTransUtil.parseStoreValue(entry)(syncTaskId)
    /**
      * 解析成功以后，根据主键分发到对应的batcher
      */
    rowChange.getRowDatasList.asScala.foreach {
      data =>
        router.fold(throw new WorkerCannotFindException({
          log.error(s"batcher router cannot be found,id:$syncTaskId")
          s"batcher router cannot be found,id:$syncTaskId"
        }))(ref => ref ! EntryKeyClassifier(entry, data, partitionStrategy))
    }
  }


  override def preStart(): Unit = {
    //状态置为offline
    initBatchers
  }

  override def postStop(): Unit = {

  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.debug(s"primaryBatcher$num process preRestart,id:$syncTaskId")
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.debug(s"primaryBatcher$num process postRestart,id:$syncTaskId")
    super.postRestart(reason)
  }

  override def supervisorStrategy = {
    AllForOneStrategy() {
      case _ => {
        Escalate
      }
    }
  }

  /**
    * 错位次数阈值
    */
  override def errorCountThreshold: Int = 1

  /**
    * 错位次数
    */
  override var errorCount: Int = _

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = ???
}

object MysqlBinlogInOrderBatcherPrimaryKeyManager {
  /**
    * 工厂方法
    *
    * @param name 加载的名称
    * @param taskManager
    * @param sinker
    * @param num
    * @return
    */
  def buildMysqlBinlogInOrderBatcherPrimaryKeyManager(name: String, taskManager: MysqlSourceManagerImp with TaskManager, sinker: ActorRef, num: Int): Props = {
    name match {
      case MysqlBinlogInOrderMysqlPrimaryKeyManager.name => MysqlBinlogInOrderMysqlPrimaryKeyManager.props(taskManager, sinker, num)
      case _ => throw new WorkerInitialFailureException(s"cannot build MysqlBinlogInOrderBatcherPrimaryKeyManager name item match $name")
    }
  }
}