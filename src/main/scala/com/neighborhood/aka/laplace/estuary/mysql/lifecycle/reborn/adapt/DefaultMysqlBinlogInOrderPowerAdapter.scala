package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.adapt

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2019/1/10.
  */
final class DefaultMysqlBinlogInOrderPowerAdapter(taskManager: TaskManager) extends MysqlBinlogInOrderPowerAdapter(taskManager)

object DefaultMysqlBinlogInOrderPowerAdapter {
  def props(taskManager: TaskManager): Props = Props(new DefaultMysqlBinlogInOrderPowerAdapter(taskManager))
}
