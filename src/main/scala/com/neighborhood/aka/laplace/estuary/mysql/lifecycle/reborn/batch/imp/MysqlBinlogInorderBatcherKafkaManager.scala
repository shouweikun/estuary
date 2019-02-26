package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp

import akka.actor.ActorRef
import com.neighborhood.aka.laplace.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderBatcherManager
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp

/**
  * Created by john_liu on 2019/2/26.
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInorderBatcherKafkaManager(
                                                   /**
                                                     * 任务信息管理器
                                                     */
                                                   override val taskManager: MysqlSourceManagerImp with TaskManager,

                                                   /**
                                                     * sinker 的ActorRef
                                                     */
                                                   override val sinker: ActorRef
                                                 ) extends MysqlBinlogInOrderBatcherManager[KafkaSinkFunc[BinlogKey, String]](taskManager, sinker) {
  /**
    * 初始化Batchers
    * 包含SpecialSender 和 普通batchers和router
    */
  override protected def initBatchers: Unit = ???
}
