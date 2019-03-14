package com.neighborhood.aka.laplace.estuary.web.controller

import com.neighborhood.aka.laplace.estuary.web.bean.Mongo2KafkaTaskRequestBean
import com.neighborhood.aka.laplace.estuary.web.service.Mongo2KafkaService
import com.neighborhood.aka.laplace.estuary.web.utils.ValidationUtils
import io.swagger.annotations.ApiOperation
import org.springframework.beans.factory.annotation.{Autowired, Qualifier}
import org.springframework.web.bind.annotation._

/**
  * Created by john_liu on 2019/01/17.
  *
  * @author neighborhood.aka.laplace
  */
@RestController
@RequestMapping(Array("/api/v1/estuary/mongo2kafka"))
final class Mongo2KafkaController extends SyncTaskController[Mongo2KafkaTaskRequestBean] {



  @Qualifier("mongo2kafka")
  @Autowired
  protected val syncService: Mongo2KafkaService = null

  @ApiOperation(value = "开始一个新的mysql2mysql任务", httpMethod = "POST", notes = "")
  @RequestMapping(value = Array("/new/sync"), method = Array(RequestMethod.POST))
  def createNewSyncTask(@RequestBody requestBody: Mongo2KafkaTaskRequestBean) = {
    ValidationUtils.notblank(requestBody.getMongo2KafkaRunningInfo.getSyncTaskId, "syncTaskId cannot be null")
    ValidationUtils.notblank(requestBody.getMongo2KafkaRunningInfo.getOffsetZookeeperServers, "offsetZookeeperServers cannot be null")
    syncService.startNewOneTaskKeepConfig(requestBody.getMongo2KafkaRunningInfo.getSyncTaskId, requestBody)
  }


  @ApiOperation(value = "查看任务状态", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/sync/status"), method = Array(RequestMethod.GET))
  def checkTaskStatus(@RequestParam("id") id: String): String = {
    ValidationUtils.notNull(id, "syncTaskId cannot be null")
    syncService.checkTaskStatus(id)
  }

  @ApiOperation(value = "查看任务配置", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/sync/config"), method = Array(RequestMethod.GET))
  def checkTaskConfig(@RequestParam("id") id: String): String = {
    ValidationUtils.notNull(id, "syncTaskId cannot be null")
    syncService.getTaskInfoConfig(id)
  }

  @ApiOperation(value = "查看所有已启动任务状态", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/sync/"), method = Array(RequestMethod.GET))
  def checkRunningTaskId(): String = {
    syncService.checkRunningTask
  }

  @ApiOperation(value = "重启任务", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("sync/restart"), method = Array(RequestMethod.GET))
  def restartTask(@RequestParam("id") id: String): Boolean = {
    ValidationUtils.notNull(id, "syncTaskId cannot be null")
    syncService.restartTask(id)
  }

  @ApiOperation(value = "停止任务", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("sync/stop"), method = Array(RequestMethod.GET))
  def stopTask(@RequestParam("id") id: String): Boolean = {
    ValidationUtils.notNull(id, "syncTaskId cannot be null")
    syncService.stopSyncTaskAndRomoveConfig(id)
  }

  @ApiOperation(value = "查看count数", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/sync/count"), method = Array(RequestMethod.GET))
  def checkCount(@RequestParam("id") id: String): String = {
    ValidationUtils.notNull(id, "syncTaskId cannot be null")
    syncService.checkLogCount(id)
  }

  @ApiOperation(value = "查看timeCost", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/sync/cost"), method = Array(RequestMethod.GET))
  def checkTimeCost(@RequestParam("id") id: String): String = {
    ValidationUtils.notNull(id, "syncTaskId cannot be null")
    syncService.checkLogCost(id)
  }

  @ApiOperation(value = "查看任务运行信息", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/sync/profiling"), method = Array(RequestMethod.GET))
  def checklastSavedlogPosition(@RequestParam("id") id: String): String = {
    ValidationUtils.notNull(id, "syncTaskId cannot be null")
    syncService.checkLastSavedLogPosition(id)
  }
}
