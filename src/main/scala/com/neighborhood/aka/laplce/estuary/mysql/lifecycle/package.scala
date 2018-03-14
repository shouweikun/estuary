package com.neighborhood.aka.laplce.estuary.mysql

import com.neighborhood.aka.laplce.estuary.core.lifecycle.Status.Status
import com.neighborhood.aka.laplce.estuary.core.lifecycle.WorkerType
import com.neighborhood.aka.laplce.estuary.core.lifecycle.WorkerType.WorkerType
import com.neighborhood.aka.laplce.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2018/2/3.
  */
package object lifecycle {

  case class BinlogPositionInfo(journalName: String, offest: Long)


}
