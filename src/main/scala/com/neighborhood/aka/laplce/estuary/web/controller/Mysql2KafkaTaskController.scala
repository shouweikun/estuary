package com.neighborhood.aka.laplce.estuary.web.controller

import com.neighborhood.aka.laplce.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.web.bean.Mysql2kafkaTaskRequestBean
import com.neighborhood.aka.laplce.estuary.web.service.Mysql2KafkaService
import io.swagger.annotations.{Api, ApiOperation}
import org.springframework.web.bind.annotation.{RequestBody, RequestMapping, RequestMethod, RestController}

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
@ApiOperation(value = "1", httpMethod = "POST", notes = "1 ")
@RequestMapping(value = Array("/new/"), method = Array(RequestMethod.POST))
  def createNewTask(@RequestBody requestBody: Mysql2kafkaTaskRequestBean) = {
      //todo 检验任务合法性
     Mysql2KafkaService.startOneTask(buildTaskInfo(requestBody))
    }

  def buildTaskInfo(requestBody: Mysql2kafkaTaskRequestBean):Mysql2KafkaTaskInfoBean = {
    val taskInfo = new Mysql2KafkaTaskInfoBean
    //监听用
    taskInfo.listenRetrytime = requestBody.getListenRetrytime
    taskInfo.listenTimeout  = requestBody.getListenTimeout
    //zookeeper用
    taskInfo.zookeeperTimeout = requestBody.getZookeeperTimeout
    taskInfo.zookeeperServers = requestBody.getZookeeperServers
    //kafka用
    taskInfo.kafkaRetries =requestBody.getKafkaRetries
    taskInfo.lingerMs = requestBody.getLingerMs
    taskInfo.bootstrapServers = requestBody.getBootstrapServers
    taskInfo.ack = requestBody.getAck
    taskInfo.topic =requestBody.getTopic
    //mysql用
    taskInfo.master = new MysqlCredentialBean(requestBody.getMysqladdress,requestBody.getMysqlPort,requestBody.getMysqlUsername,requestBody.getMysqlPassword,requestBody.getMysqlDefaultDatabase)
    //过滤用
    taskInfo.filterBlackPattern = requestBody.getFilterBlackPattern
    taskInfo.filterPattern = requestBody.getFilterPattern
    taskInfo.filterQueryDcl = requestBody.isFilterQueryDcl
    taskInfo.filterQueryDml = requestBody.isFilterQueryDml
    taskInfo.filterQueryDdl = requestBody.isFilterQueryDdl
    taskInfo.eventBlackFilterPattern = requestBody.getEventBlackFilterPattern
    taskInfo.eventFilterPattern =requestBody.getEventFilterPattern
    //开始的position
    taskInfo.journalName = requestBody.getJournalName
    taskInfo.position = requestBody.getPosition
    //模式设置
    taskInfo.isProfiling = requestBody.isProfiling
    taskInfo.isTransactional = requestBody.isTransactional
    taskInfo.isCounting = requestBody.isCounting
    //其他
    taskInfo.batchThreshold.set(requestBody.getBatchThreshold)

    taskInfo
  }
}
