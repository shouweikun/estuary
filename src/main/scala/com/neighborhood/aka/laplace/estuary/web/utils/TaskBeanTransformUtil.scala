package com.neighborhood.aka.laplace.estuary.web.utils

import com.neighborhood.aka.laplace.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplace.estuary.web.bean.Mysql2kafkaTaskRequestBean
import org.springframework.util.StringUtils

import scala.collection.JavaConverters._

object TaskBeanTransformUtil {

  def transform2Mysql2KafkaTaskInfoBean(requestBody: Mysql2kafkaTaskRequestBean): Mysql2KafkaTaskInfoBean = {
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
    if (!StringUtils.isEmpty(requestBody.getEventBlackFilterPattern))
      taskInfo.eventBlackFilterPattern = requestBody.getEventBlackFilterPattern
    if (!StringUtils.isEmpty(requestBody.getEventFilterPattern))
      taskInfo.eventFilterPattern = requestBody.getEventFilterPattern
    //开始的position
    if (!StringUtils.isEmpty(requestBody.getBinlogJournalName)) {
      taskInfo.journalName = requestBody.getBinlogJournalName
      taskInfo.position = requestBody.getBinlogPosition
    }
    taskInfo.timestamp = requestBody.getBinlogTimeStamp
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

    taskInfo.concernedDatabase = requestBody.getConcernedDataBase
    taskInfo.ignoredDatabase = requestBody.getIgnoredDataBase
    taskInfo
  }
}
