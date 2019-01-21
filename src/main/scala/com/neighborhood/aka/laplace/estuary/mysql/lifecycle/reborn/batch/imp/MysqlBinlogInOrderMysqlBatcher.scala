package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.MysqlBinlogInOrderBatcher
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{EntryKeyClassifier, MysqlRowDataInfo}
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp

/**
  * Created by john_liu on 2019/1/15.
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInOrderMysqlBatcher(override val taskManager: MysqlSourceManagerImp with TaskManager,
                                           override val sinker: ActorRef,
                                           override val num: Int) extends MysqlBinlogInOrderBatcher[MysqlRowDataInfo](taskManager, sinker, num) {

  override val mappingFormat: MappingFormat[EntryKeyClassifier, MysqlRowDataInfo] = taskManager.batchMappingFormat.get.asInstanceOf[MappingFormat[EntryKeyClassifier, MysqlRowDataInfo]]

}

object MysqlBinlogInOrderMysqlBatcher {
  val name: String = MysqlBinlogInOrderMysqlBatcher.getClass.getName.stripSuffix("$")

  def props(taskManager: MysqlSourceManagerImp with TaskManager,
            sinker: ActorRef,
            num: Int): Props = Props(new MysqlBinlogInOrderMysqlBatcher(taskManager, sinker, num))
}
