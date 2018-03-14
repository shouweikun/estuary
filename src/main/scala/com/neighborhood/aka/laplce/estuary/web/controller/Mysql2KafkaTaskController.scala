package com.neighborhood.aka.laplce.estuary.web.controller

import com.neighborhood.aka.laplce.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.web.bean.Mysql2kafkaTaskRequestBean
import com.neighborhood.aka.laplce.estuary.web.service.Mysql2KafkaService
import io.swagger.annotations.ApiOperation
import org.springframework.web.bind.annotation._

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
    //todo 检验任务合法性
    Mysql2KafkaService.startOneTask(buildTaskInfo(requestBody))
  }

  @ApiOperation(value = "查看任务状态", httpMethod = "GET", notes = "")
  @RequestMapping(value = Array("/check"), method = Array(RequestMethod.GET))
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

  def buildTaskInfo(requestBody: Mysql2kafkaTaskRequestBean): Mysql2KafkaTaskInfoBean = {
    val taskInfo = new Mysql2KafkaTaskInfoBean
    //任务id
    taskInfo.syncTaskId = requestBody.getSyncTaskId
    //监听用
    taskInfo.listenRetrytime = requestBody.getListenRetrytime
    taskInfo.listenTimeout = requestBody.getListenTimeout
    //zookeeper用
    taskInfo.zookeeperTimeout = requestBody.getZookeeperTimeout
    taskInfo.zookeeperServers = requestBody.getZookeeperServers
    //kafka用
    taskInfo.kafkaRetries = requestBody.getKafkaRetries
    taskInfo.lingerMs = requestBody.getKafkaLingerMs
    taskInfo.bootstrapServers = requestBody.getKafkaBootstrapServers
    taskInfo.ack = requestBody.getKafkaAck
    taskInfo.topic = requestBody.getKafkaTopic
    //mysql用
    taskInfo.master = new MysqlCredentialBean(requestBody.getMysqladdress, requestBody.getMysqlPort, requestBody.getMysqlUsername, requestBody.getMysqlPassword, requestBody.getMysqlDefaultDatabase)
    //过滤用
    taskInfo.filterBlackPattern = requestBody.getFilterBlackPattern
    taskInfo.filterPattern = requestBody.getFilterPattern
    taskInfo.filterQueryDcl = requestBody.isFilterQueryDcl
    taskInfo.filterQueryDml = requestBody.isFilterQueryDml
    taskInfo.filterQueryDdl = requestBody.isFilterQueryDdl
    taskInfo.eventBlackFilterPattern = requestBody.getEventBlackFilterPattern
    taskInfo.eventFilterPattern = requestBody.getEventFilterPattern
    //开始的position
    taskInfo.journalName = requestBody.getBinlogJournalName
    taskInfo.position = requestBody.getBinlogPosition
    //模式设置
    taskInfo.isProfiling = requestBody.isProfiling
    taskInfo.isTransactional = requestBody.isTransactional
    taskInfo.isCounting = requestBody.isCounting
    //其他
    taskInfo.batchThreshold.set(requestBody.getBatchThreshold)

    taskInfo
  }
}
