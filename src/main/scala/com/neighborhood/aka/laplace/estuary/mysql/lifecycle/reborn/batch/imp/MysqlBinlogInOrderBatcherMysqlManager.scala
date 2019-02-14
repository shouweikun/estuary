package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp

import akka.actor.{ActorRef, Props}
import akka.routing.{ConsistentHashingGroup, RoundRobinGroup}
import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.{MysqlBinlogInOrderBatcherManager, MysqlBinlogInOrderBatcherPrimaryKeyManager, MysqlBinlogInOrderSpecialInfoSender}
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp

/**
  * Created by john_liu on 2019/1/14.
  */
final class MysqlBinlogInOrderBatcherMysqlManager(
                                                   /**
                                                     * 任务信息管理器
                                                     */
                                                   override val taskManager: MysqlSourceManagerImp with TaskManager,

                                                   /**
                                                     * sinker 的ActorRef
                                                     */
                                                   override val sinker: ActorRef
                                                 ) extends MysqlBinlogInOrderBatcherManager(taskManager, sinker) {
  /**
    * 初始化Batchers
    */
  override protected def initBatchers: Unit = {
    taskManager.wait4SinkerList() //必须要等待,一定要等sinkerList创建完毕才行


    val name = MysqlBinlogInOrderMysqlPrimaryKeyManager.name
    val paths: List[String] = (1 to batcherNum).map {
      index =>
        MysqlBinlogInOrderBatcherPrimaryKeyManager.buildMysqlBinlogInOrderBatcherPrimaryKeyManager(name, taskManager, taskManager.sinkerList(index - 1), index)
    }.map(context.actorOf(_)).map(_.path.toString).toList
    lazy val roundRobin = context.actorOf(new RoundRobinGroup(paths).props().withDispatcher("akka.batcher-dispatcher"), "router")
    lazy val consistentHashing = context.actorOf(new ConsistentHashingGroup(paths, virtualNodesFactor = SettingConstant.HASH_MAPPING_VIRTUAL_NODES_FACTOR).props().withDispatcher("akka.batcher-dispatcher"), routerName)
    partitionStrategy match { //暂未支持其他分区等级
      case PartitionStrategy.PRIMARY_KEY => consistentHashing
      case PartitionStrategy.DATABASE_TABLE => consistentHashing
    }


    val specialInfoSenderTypeName = batcherNameToLoad.get(specialInfoSenderName).getOrElse(MysqlBinlogInOrderMysqlSpecialInfoSender.name)
    val props = MysqlBinlogInOrderSpecialInfoSender.buildMysqlBinlogInOrderSpecialInfoSender(specialInfoSenderTypeName, taskManager, sinker)
    context.actorOf(props, specialInfoSenderName)
  }
}

object MysqlBinlogInOrderBatcherMysqlManager {
  lazy val name: String = MysqlBinlogInOrderBatcherMysqlManager.getClass.getName.stripSuffix("$")

  def props(taskManager: MysqlSourceManagerImp with TaskManager, sinker: ActorRef): Props = Props(new MysqlBinlogInOrderBatcherMysqlManager(taskManager, sinker))
}