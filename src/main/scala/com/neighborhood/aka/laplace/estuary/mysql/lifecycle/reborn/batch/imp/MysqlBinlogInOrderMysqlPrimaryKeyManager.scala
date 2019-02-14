package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp

import akka.actor.{ActorRef, Props}
import akka.routing.{ConsistentHashingGroup, RoundRobinGroup}
import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.SettingConstant
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.{MysqlBinlogInOrderBatcher, MysqlBinlogInOrderBatcherPrimaryKeyManager}
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp

/**
  * Created by john_liu on 2019/1/15.
  * MysqlSinkFunc 对应的PrimaryKeyManager
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInOrderMysqlPrimaryKeyManager(
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
                                                    ) extends MysqlBinlogInOrderBatcherPrimaryKeyManager[MysqlSinkFunc](taskManager, sinker, num) {


  /**
    * 初始化Batchers
    */
  override protected def initBatchers: Unit = {
    val sinkerList = taskManager.sinkerList
    val paths = (0 until batcherNum).map { index =>
      val i = if (partitionStrategy == PartitionStrategy.DATABASE_TABLE) num-1 else index
      MysqlBinlogInOrderBatcher.buildMysqlBinlogInOrderBatcher(MysqlBinlogInOrderMysqlBatcher.name, taskManager, sinkerList(i), index)
    }.map(context.actorOf(_)).map(_.path.toString).toList

    lazy val roundRobin = context.actorOf(new RoundRobinGroup(paths).props().withDispatcher("akka.batcher-dispatcher"), "router")
    lazy val consistentHashing = context.actorOf(new ConsistentHashingGroup(paths, virtualNodesFactor = SettingConstant.HASH_MAPPING_VIRTUAL_NODES_FACTOR).props().withDispatcher("akka.batcher-dispatcher"), routerName)
    partitionStrategy match {
      //          case PartitionStrategy.MOD => roundRobin
      case PartitionStrategy.DATABASE_TABLE => consistentHashing
      case PartitionStrategy.PRIMARY_KEY => consistentHashing
      //          case PartitionStrategy.TRANSACTION => context.actorOf(MysqlBinlogInOrderBatcher.props(taskManager, sinker, 1), s"router")
    }
  }
}

object MysqlBinlogInOrderMysqlPrimaryKeyManager {
  val name: String = MysqlBinlogInOrderMysqlPrimaryKeyManager.getClass.getName.stripSuffix("$")

  def props(

             taskManager: MysqlSourceManagerImp with TaskManager
             ,
             sinker: ActorRef
             ,
             num: Int
           ): Props = Props(new MysqlBinlogInOrderMysqlPrimaryKeyManager(taskManager,
    sinker,
    num
  ))
}