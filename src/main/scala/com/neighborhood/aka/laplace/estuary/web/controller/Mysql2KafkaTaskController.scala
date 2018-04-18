package com.neighborhood.aka.laplace.estuary.web.controller

import com.neighborhood.aka.laplace.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplace.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplace.estuary.web.bean.Mysql2kafkaTaskRequestBean
import com.neighborhood.aka.laplace.estuary.web.service.Mysql2KafkaService
import com.neighborhood.aka.laplace.estuary.web.utils.ValidationUtils
import io.swagger.annotations.ApiOperation
import org.springframework.util.StringUtils
import org.springframework.web.bind.annotation._

import scala.collection.JavaConverters._

/**
  * Created by john_liu on 2018/3/10.
  */
@RestController
@RequestMapping(Array("/api/v1/estuary/mysql2kafka"))
class Mysql2KafkaTaskController {


  //    @GetMapping(Array(""))
  //    def getAllSparkJobEntity() = {
  //      val all = sparkEntityDao.findAll().toList.map(_.toView())
  //      JsonHelper.to(all)
  //    }
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
    /** *****************************************************/


    Mysql2KafkaService.startNewOneTask(buildTaskInfo(requestBody))
  }

  @ApiOperation(value = "查看任务状态", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check/task/status"), method = Array(RequestMethod.GET))
  def checkTaskStatus(@RequestParam("id") id: String): String = {
    Mysql2KafkaService.checkTaskStatus(id)
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

  def buildTaskInfo(requestBody: Mysql2kafkaTaskRequestBean): Mysql2KafkaTaskInfoBean = {
    val taskInfo = new Mysql2KafkaTaskInfoBean
    //任务id
    taskInfo.syncTaskId = requestBody.getSyncTaskId
    //监听用
    taskInfo.listenRetrytime = requestBody.getListenRetrytime
    if (requestBody.getListenTimeout > 0) taskInfo.listenTimeout = requestBody.getListenTimeout
    //zookeeper用
    if (requestBody.getZookeeperTimeout > 0) taskInfo.zookeeperTimeout = requestBody.getZookeeperTimeout
    taskInfo.zookeeperServers = requestBody.getZookeeperServers
    //kafka用
    if (!StringUtils.isEmpty(requestBody.getKafkaRetries) && requestBody.getKafkaRetries.toInt >= 0) taskInfo.kafkaRetries = requestBody.getKafkaRetries
    if (!StringUtils.isEmpty(requestBody.getKafkaLingerMs) && requestBody.getKafkaLingerMs.toInt >= 0) taskInfo.lingerMs = requestBody.getKafkaLingerMs
    taskInfo.bootstrapServers = requestBody.getKafkaBootstrapServers
    if (!StringUtils.isEmpty(requestBody.getKafkaAck) && requestBody.getKafkaAck.toInt >= 1) taskInfo.ack = requestBody.getKafkaAck
    taskInfo.topic = requestBody.getKafkaTopic
    taskInfo.ddlTopic = requestBody.getKafkaDdlTopic
    if (requestBody.getKafkaSpecficTopics != null)
      taskInfo.specificTopics = requestBody
        .getKafkaSpecficTopics
        .asScala //转化为scala集合
        .flatMap {
        kv =>
          kv._2 // 库表名
            .split(",") //通过`,`逗号分隔
            .map(newK => newK -> kv._1) //转换为`库表 -> topic`的形式
      }.toMap
    //mysql用
    taskInfo.master = new MysqlCredentialBean(requestBody.getMysqladdress, requestBody.getMysqlPort, requestBody.getMysqlUsername, requestBody.getMysqlPassword, requestBody.getMysqlDefaultDatabase)
    //过滤用
    taskInfo.filterBlackPattern = requestBody.getFilterBlackPattern
    taskInfo.filterPattern = requestBody.getFilterPattern
    taskInfo.filterQueryDcl = requestBody.isFilterQueryDcl
    taskInfo.filterQueryDml = requestBody.isFilterQueryDml
    taskInfo.filterQueryDdl = requestBody.isFilterQueryDdl
    taskInfo.concernedDatabase = requestBody.getConcernedDataBase
    taskInfo.ignoredDatabase = requestBody.getIgnoredDataBase
    if (!StringUtils.isEmpty(requestBody.getEventBlackFilterPattern))
      taskInfo.eventBlackFilterPattern = requestBody.getEventBlackFilterPattern
    if (!StringUtils.isEmpty(requestBody.getEventFilterPattern))
      taskInfo.eventFilterPattern = requestBody.getEventFilterPattern
    //开始的position
    if (!StringUtils.isEmpty(requestBody.getBinlogJournalName)) {
      taskInfo.journalName = requestBody.getBinlogJournalName
      taskInfo.position = requestBody.getBinlogPosition
    }
    //模式设置
    taskInfo.isCosting = requestBody.isCosting
    taskInfo.isTransactional = requestBody.isTransactional
    taskInfo.isCounting = requestBody.isCounting
    taskInfo.isProfiling = requestBody.isProfiling
    List(requestBody.isCosting, requestBody.isCounting, requestBody.isProfiling).forall(x => x) match {
      case true => taskInfo.isPowerAdapted = requestBody.isPowerAdapted
      case _ => {}
    }
    //batcher
    if (requestBody.getBatchThreshold > 0) {
      taskInfo.batchThreshold.set(requestBody.getBatchThreshold)
    }
    if (requestBody.getBatcherCount > 0) {
      taskInfo.batcherNum = requestBody.getBatcherCount
    }
    //fetcher
    taskInfo.fetchDelay.set(requestBody.getFetchDelay)
    taskInfo
  }
}
