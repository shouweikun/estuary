package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.adapt

import akka.actor.Props
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2019/1/10.
  *
  * @author neighborhood.aka.laplace
  */
final class DefaultOplogPowerAdapter(taskManager: TaskManager) extends OplogPowerAdapter(taskManager)

object DefaultOplogPowerAdapter {
  val name = DefaultOplogPowerAdapter.getClass.getName.stripSuffix("$")

  def props(taskManager: TaskManager): Props = Props(new DefaultOplogPowerAdapter(taskManager))
}
