package com.neighborhood.aka.laplace.estuary.web.controller

import com.neighborhood.aka.laplace.estuary.web.bean.TaskRequestBean
import com.neighborhood.aka.laplace.estuary.web.service.SyncService
import org.springframework.web.bind.annotation.{RequestBody, RequestParam}

/**
  * Created by john_liu on 2019/3/13.
  */
trait SyncTaskController[A <: TaskRequestBean] {

  protected val syncService: SyncService[A]

  def createNewSyncTask(@RequestBody requestBody: A)


  def checkTaskStatus(@RequestParam("id") id: String): String

  def checkTaskConfig(@RequestParam("id") id: String): String

  def checkRunningTaskId(): String

  def restartTask(@RequestParam("id") id: String): Boolean

  def stopTask(@RequestParam("id") id: String): Boolean

  def checkCount(@RequestParam("id") id: String): String

  def checkTimeCost(@RequestParam("id") id: String): String

  def checklastSavedlogPosition(@RequestParam("id") id: String): String
}
