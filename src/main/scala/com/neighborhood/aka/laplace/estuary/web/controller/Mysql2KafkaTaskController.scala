package com.neighborhood.aka.laplace.estuary.web.controller

import com.neighborhood.aka.laplace.estuary.web.bean.{Mysql2kafkaTaskRequestBean, SnapshotRequestBean}
import com.neighborhood.aka.laplace.estuary.web.service.Mysql2KafkaService
import com.neighborhood.aka.laplace.estuary.web.utils.ValidationUtils
import io.swagger.annotations.ApiOperation
import org.springframework.web.bind.annotation._

/**
  * Created by john_liu on 2018/3/10.
  */
@RestController
@RequestMapping(Array("/api/v1/estuary/mysql2kafka"))
final class Mysql2KafkaTaskController {

  @ApiOperation(value = "开始一个新的snapshot任务", httpMethod = "POST", notes = "")
  @RequestMapping(value = Array("/new/snapshot"), method = Array(RequestMethod.POST))
  def createNewSnapshotTask(@RequestBody requestBody: SnapshotRequestBean) = {
    /** ******************************************************/
    ValidationUtils.notNull(requestBody.getSyncTaskId, "syncTaskId cannot be null ")
    ValidationUtils.notblank(requestBody.getSyncTaskId, "syncTaskId  cannot be blank ")
    ValidationUtils.notNull(requestBody.getSnapshotTaskId, "SnapshotTaskId cannot be null ")
    ValidationUtils.notblank(requestBody.getSnapshotTaskId, "SnapshotTaskId  cannot be blank ")
    ValidationUtils.notZero(requestBody.getTargetTimestamp, "TaskType cannot be zero")
    /** *****************************************************/

    Mysql2KafkaService.startNewSnapshotTask(requestBody)
  }

  @ApiOperation(value = "开始一个新的mysql2kafka任务", httpMethod = "POST", notes = "")
  @RequestMapping(value = Array("/new/sync"), method = Array(RequestMethod.POST))
  def createNewSyncTask(@RequestBody requestBody: Mysql2kafkaTaskRequestBean) = {
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
    ValidationUtils.notZero(requestBody.getTaskType, "TaskType cannot be zero")

    //    ValidationUtils.notblank(requestBody.getHbaseZookeeperQuorum,"Hbase Zookeeper Quorum cannot be blank")
    //    ValidationUtils.notblank(requestBody.getHbaseZookeeperPropertyClientPort,"Hbase Zookeeper Property Client Port cannot be blank")
    ValidationUtils.notNull(requestBody.getConcernedDataBase, "concerned database cannot be null")
    ValidationUtils.notblank(requestBody.getConcernedDataBase, "concerned database cannot be blank")
    ValidationUtils.notNull(requestBody.getTaskStartTime, "taskStartTimeCannot be null")
    /** *****************************************************/


    Mysql2KafkaService.startNewOneTask(requestBody)
  }

  @ApiOperation(value = "查看快照任务状态", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/snapshot/status"), method = Array(RequestMethod.GET))
  def checkSnapshotTaskStatus(@RequestParam("id") id: String): String = {
    Mysql2KafkaService.checkSnapshotTaskRunningInfo(id)
  }

  @ApiOperation(value = "查看任务状态", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/sync/status"), method = Array(RequestMethod.GET))
  def checkTaskStatus(@RequestParam("id") id: String): String = {
    Mysql2KafkaService.checkTaskStatus(id)
  }

  @ApiOperation(value = "查看任务配置", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/sync/config"), method = Array(RequestMethod.GET))
  def checkTaskConfig(@RequestParam("id") id: String): String = {
    Mysql2KafkaService.getEstuaryConfigString(id)
  }

  @ApiOperation(value = "查看所有已启动任务状态", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/sync/"), method = Array(RequestMethod.GET))
  def checkRunningTaskId(): String = {
    Mysql2KafkaService.checkRunningTaskIds
  }

  @ApiOperation(value = "重启任务", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("sync/restart"), method = Array(RequestMethod.GET))
  def restartTask(@RequestParam("id") id: String): Boolean = {
    Mysql2KafkaService.reStartTask(id)
  }

  @ApiOperation(value = "停止任务", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("sync/stop"), method = Array(RequestMethod.GET))
  def stopTask(@RequestParam("id") id: String): Boolean = {
    Mysql2KafkaService.stopTask(id)
  }

  @ApiOperation(value = "查看count数", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/sync/count"), method = Array(RequestMethod.GET))
  def checkCount(@RequestParam("id") id: String): String = {
    Mysql2KafkaService.checklogCount(id)
  }

  @ApiOperation(value = "查看timeCost", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/sync/cost"), method = Array(RequestMethod.GET))
  def checkTimeCost(@RequestParam("id") id: String): String = {
    Mysql2KafkaService.checkTimeCost(id)
  }

  @ApiOperation(value = "查看任务运行信息", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/sync/profiling"), method = Array(RequestMethod.GET))
  def checklastSavedlogPosition(@RequestParam("id") id: String): String = {
    Mysql2KafkaService.checkLastSavedLogPosition(id)
  }
}
