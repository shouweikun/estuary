package com.neighborhood.aka.laplace.estuary.web.controller

import com.neighborhood.aka.laplace.estuary.web.bean.Mysql2kafkaTaskRequestBean
import com.neighborhood.aka.laplace.estuary.web.service.Mysql2KafkaService
import com.neighborhood.aka.laplace.estuary.web.utils.ValidationUtils
import io.swagger.annotations.ApiOperation
import org.springframework.web.bind.annotation._

/**
  * Created by john_liu on 2018/3/10.
  */
@RestController
@RequestMapping(Array("/api/v1/estuary/mysql2kafka"))
class Mysql2KafkaTaskController {


  @ApiOperation(value = "开始一个已经存在的mysql2kafka任务", httpMethod = "POST", notes = "")
  @RequestMapping(value = Array("/start/exit/onetask"), method = Array(RequestMethod.POST))
  def startExitOne(@RequestParam("key") key: String, @RequestParam("value") value: String) = {
    Mysql2KafkaService.startOneExistTask(key, value)
  }

  @ApiOperation(value = "根据syncTaskId重新启动mysql2kafka任务", httpMethod = "POST", notes = "")
  @RequestMapping(value = Array("/start/exit/onetask/synctaskid"), method = Array(RequestMethod.POST))
  def startExitSyncTaskId(@RequestParam("syncTaskId") syncTaskId: String) = {
    Mysql2KafkaService.startOneExistTask("syncTaskId", syncTaskId)
  }

  @ApiOperation(value = "启动所有已经存在的mysql2kafka任务", httpMethod = "POST", notes = "")
  @RequestMapping(value = Array("/start/exit/alltasks"), method = Array(RequestMethod.POST))
  def startAllExitTasks() = {
    Mysql2KafkaService.startAllExistTask
  }

  @ApiOperation(value = "开始一个新的mysql2kafka任务", httpMethod = "POST", notes = "")
  @RequestMapping(value = Array("/new/"), method = Array(RequestMethod.POST))
  def createNewTask(@RequestBody requestBody: Mysql2kafkaTaskRequestBean) = {
    /** ******************************************************/
    ValidationUtils.notNull(requestBody.getKafkaBootstrapServers, "KafkaBootstrapServers cannot be null ")
    ValidationUtils.notblank(requestBody.getKafkaBootstrapServers, "KafkaBootstrapServers cannot be blank ")
    ValidationUtils.notNull(requestBody.getKafkaTopic, "kafkaTopic cannot be null")
    ValidationUtils.notblank(requestBody.getKafkaTopic, "kafkaTopic cannot be null")
    ValidationUtils.notNull(requestBody.getKafkaDdlTopic, "kafkaDdlTopic cannot be null")
    ValidationUtils.notblank(requestBody.getKafkaDdlTopic, "kafkaDdlTopic cannot be null")
    ValidationUtils.notNull(requestBody.getMysqladdress, "Mysqladdress cannot be null")
    ValidationUtils.notblank(requestBody.getMysqladdress, "Mysqladdress cannot be blank")
    ValidationUtils.notNull(requestBody.getMysqladdress, "Mysqladdress cannot be null")
    ValidationUtils.notblank(requestBody.getMysqladdress, "Mysqladdress cannot be blank")
    ValidationUtils.notNull(requestBody.getMysqlUsername, "MysqlUsername cannot be null")
    ValidationUtils.notblank(requestBody.getMysqlUsername, "MysqlUsername cannot be blank")
    ValidationUtils.notNull(requestBody.getMysqlPassword, "MysqlPassword cannot be null")
    ValidationUtils.notblank(requestBody.getMysqlUsername, "MysqlPassword cannot be blank")
    ValidationUtils.notNull(requestBody.getSyncTaskId, "SyncTaskId cannot be null")
    ValidationUtils.notblank(requestBody.getSyncTaskId, "SyncTaskId cannot be null")
    ValidationUtils.notNull(requestBody.getZookeeperServers, "ZookeeperServers cannot be null")
    ValidationUtils.notblank(requestBody.getZookeeperServers, "ZookeeperServers cannot be blank")
    ValidationUtils.notZero(requestBody.getTaskType,"TaskType cannot be zero")
    /** *****************************************************/


    Mysql2KafkaService.startNewOneTask(requestBody)
  }

  @ApiOperation(value = "查看任务状态", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/task/status"), method = Array(RequestMethod.GET))
  def checkTaskStatus(@RequestParam("id") id: String): String = {
    Mysql2KafkaService.checkTaskStatus(id)
  }

  @ApiOperation(value = "查看所有已启动任务状态", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/task/"), method = Array(RequestMethod.GET))
  def checkRunningTaskId(): String = {
    Mysql2KafkaService.checkRunningTaskIds
  }

  @ApiOperation(value = "重启任务", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/restart"), method = Array(RequestMethod.GET))
  def restartTask(@RequestParam("id") id: String): Boolean = {
    Mysql2KafkaService.reStartTask(id)
  }

  @ApiOperation(value = "停止任务", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/stop"), method = Array(RequestMethod.GET))
  def stopTask(@RequestParam("id") id: String): Boolean = {
    Mysql2KafkaService.stopTask(id)
  }

  @ApiOperation(value = "查看AkkaSystem状态", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/system/status"), method = Array(RequestMethod.GET))
  def checkSystemStatus(): String = {
    Mysql2KafkaService.checkSystemStatus
  }

  @ApiOperation(value = "查看count数", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/task/count"), method = Array(RequestMethod.GET))
  def checkCount(@RequestParam("id") id: String): String = {
    Mysql2KafkaService.checklogCount(id)
  }

  @ApiOperation(value = "查看timeCost", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/task/cost"), method = Array(RequestMethod.GET))
  def checkTimeCost(@RequestParam("id") id: String): String = {
    Mysql2KafkaService.checkTimeCost(id)
  }

  @ApiOperation(value = "查看", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/task/profiling"), method = Array(RequestMethod.GET))
  def checklastSavedlogPosition(@RequestParam("id") id: String): String = {
    Mysql2KafkaService.checklastSavedlogPosition(id)
  }
}
