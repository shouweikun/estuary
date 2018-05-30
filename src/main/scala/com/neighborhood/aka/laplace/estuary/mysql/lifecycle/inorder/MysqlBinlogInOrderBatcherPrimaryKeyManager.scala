package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.inorder

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{ActorRef, AllForOneStrategy, Props}
import akka.routing.{ConsistentHashingGroup, RoundRobinGroup}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.bean.exception.batch.StoreValueParseFailureException
import com.neighborhood.aka.laplace.estuary.bean.exception.control.WorkerCannotFindException
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataBatcherManagerPrototype
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{DatabaseAndTableNameClassifier, IdClassifier}
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.utils.CanalEntryJsonHelper

import scala.util.Try

/**
  * Created by john_liu on 2018/5/8.
  */
class MysqlBinlogInOrderBatcherPrimaryKeyManager(
                                                  /**
                                                    * 任务信息管理器
                                                    */
                                                  override val taskManager: Mysql2KafkaTaskInfoManager,

                                                  /**
                                                    * sinker 的ActorRef
                                                    */
                                                  override val sinker: ActorRef,

                                                  /**
                                                    * 编号
                                                    */
                                                  override val num: Int = -1
                                                ) extends SourceDataBatcherManagerPrototype {

  /**
    * 是否是最上层的manager
    */
  override val isHead: Boolean = false
  /**
    * 同步任务id
    */
  override val syncTaskId = taskManager.syncTaskId
  /**
    * 是否同步写
    */
  val isSync = taskManager.isSync
  //  val mysqlMetaConnection = mysql2KafkaTaskInfoManager.mysqlConnection.fork
  //  var tableMetaCache = buildTableMeta
  /**
    * batcher的数量
    */
  val batcherNum = taskManager.batcherNum
  /**
    * router的ActorRef
    */
  lazy val router = context.child("router")

  override def receive: Receive = {
    case DatabaseAndTableNameClassifier(entry) => {
      import scala.collection.JavaConverters._
      def parseError = {
        log.error(s"parse row data error,id:${syncTaskId}");
        throw new StoreValueParseFailureException(s"parse row data error entry:${CanalEntryJsonHelper.entryToJson(entry)},id:${syncTaskId}")
      }

      val rowChange = Try(CanalEntry.RowChange.parseFrom(entry.getStoreValue)).toOption.getOrElse(parseError)
      /**
        * 解析成功以后，根据主键分发到对应的batcher
        */
      rowChange.getRowDatasList.asScala.foreach {
        data =>
          router.fold(throw new WorkerCannotFindException({
            log.error(s"batcher router cannot be found,id:$syncTaskId");
            s"batcher router cannot be found,id:$syncTaskId"
          }))(ref => ref ! IdClassifier(entry, data))
      }


    }
  }

  def initBatchers: Unit = {
    //编号从1 开始
    lazy val paths = (1 to batcherNum)
      .map(index => context.actorOf(MysqlBinlogInOrderBatcher.props(taskManager, sinker, index), s"batcher$index").path.toString)
    if (isSync) {
      context.actorOf(new ConsistentHashingGroup(paths, virtualNodesFactor = SettingConstant.HASH_MAPPING_VIRTUAL_NODES_FACTOR).props().withDispatcher("akka.batcher-dispatcher"), "router")
    } else {
      context.actorOf(new RoundRobinGroup(paths).props().withDispatcher("akka.batcher-dispatcher"), "router")
    }

  }


  override def preStart(): Unit = {
    //状态置为offline
    initBatchers
  }

  override def postStop(): Unit = {

  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"primaryBatcher process preRestart,id:$syncTaskId")
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"primaryBatcher process postRestart,id:$syncTaskId")
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

object MysqlBinlogInOrderBatcherPrimaryKeyManager {
  def props(
             mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,
             sinker: ActorRef,
             num: Int
           ): Props = Props(new MysqlBinlogInOrderBatcherPrimaryKeyManager(mysql2KafkaTaskInfoManager, sinker, num))


}