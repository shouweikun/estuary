package com.neighborhood.aka.laplace.estuary.web.controller

import com.neighborhood.aka.laplace.estuary.web.bean.Mongo2KafkaTaskRequestBean
import com.neighborhood.aka.laplace.estuary.web.service.SyncService
import org.springframework.beans.factory.annotation.{Autowired, Qualifier}

/**
  * Created by john_liu on 2019/3/15.
  */
final class Mongo2HBaseController extends SyncTaskController[Mongo2KafkaTaskRequestBean] {

  @Qualifier("mongo2hbase")
  @Autowired
  override protected val syncService: SyncService[Mongo2KafkaTaskRequestBean] = null

  override def createNewSyncTask(requestBody: Mongo2KafkaTaskRequestBean): Unit = ???

  override def checkTaskStatus(id: String): String = ???

  override def checkTaskConfig(id: String): String = ???

  override def checkRunningTaskId(): String = ???

  override def restartTask(id: String): Boolean = ???

  override def stopTask(id: String): Boolean = ???

  override def checkCount(id: String): String = ???

  override def checkTimeCost(id: String): String = ???

  override def checklastSavedlogPosition(id: String): String = ???
}
