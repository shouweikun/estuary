package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch

import akka.actor.{ActorRef, Props}
import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp

/**
  * Created by john_liu on 2019/1/12.
  *
  * @author neighborhood.aka.laplace
  */
final class DefaultMysqlBinlogInOrderDirectFetcher(
                                                    override val taskManager: MysqlSourceManagerImp with TaskManager,
                                                    override val downStream: ActorRef) extends MysqlBinlogInOrderDirectFetcher(taskManager, downStream) {
  override protected def executeDdl(entry: CanalEntry.Entry): Unit = throw new UnsupportedOperationException(s"DefaultMysqlBinlogInOrderDirectFetcher does not support execute ddl,:id:$syncTaskId ")
}

object DefaultMysqlBinlogInOrderDirectFetcher {
  val name: String = DefaultMysqlBinlogInOrderDirectFetcher.getClass.getName.stripSuffix("$")

  def props(taskManager: MysqlSourceManagerImp with TaskManager
            , downStream: ActorRef
           ): Props = Props(new DefaultMysqlBinlogInOrderDirectFetcher(
    taskManager,
    downStream
  ))
}
