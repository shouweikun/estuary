package com.neighborhood.aka.laplace.estuary.mysql

import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.WorkerType
import com.neighborhood.aka.laplace.estuary.core.lifecycle.WorkerType.WorkerType
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager

/**
  * Created by john_liu on 2018/2/3.
  */
package object lifecycle {

  case class BinlogPositionInfo(journalName: String, offest: Long)


}
