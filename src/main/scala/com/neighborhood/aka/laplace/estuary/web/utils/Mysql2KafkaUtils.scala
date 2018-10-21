package com.neighborhood.aka.laplace.estuary.web.utils

import com.neighborhood.aka.laplace.estuary.web.bean.Mysql2kafkaTaskRequestBean

/**
  * @author laomei on 2018/10/21 13:44
  */
object Mysql2KafkaUtils {

  def validateMysql2KafkaTaskConfiguration(requestBean: Mysql2kafkaTaskRequestBean): Unit = {
    ValidationUtils.notNull(requestBean.getKafkaBootstrapServers, "KafkaBootstrapServers cannot be null ")
    ValidationUtils.notblank(requestBean.getKafkaBootstrapServers, "KafkaBootstrapServers cannot be blank ")
    ValidationUtils.notNull(requestBean.getKafkaTopic, "kafkaTopic cannot be null")
    ValidationUtils.notblank(requestBean.getKafkaTopic, "kafkaTopic cannot be null")
    ValidationUtils.notNull(requestBean.getKafkaDdlTopic, "kafkaDdlTopic cannot be null")
    ValidationUtils.notblank(requestBean.getKafkaDdlTopic, "kafkaDdlTopic cannot be null")
    ValidationUtils.notNull(requestBean.getMysqladdress, "Mysqladdress cannot be null")
    ValidationUtils.notblank(requestBean.getMysqladdress, "Mysqladdress cannot be blank")
    ValidationUtils.notNull(requestBean.getMysqladdress, "Mysqladdress cannot be null")
    ValidationUtils.notblank(requestBean.getMysqladdress, "Mysqladdress cannot be blank")
    ValidationUtils.notNull(requestBean.getMysqlUsername, "MysqlUsername cannot be null")
    ValidationUtils.notblank(requestBean.getMysqlUsername, "MysqlUsername cannot be blank")
    ValidationUtils.notNull(requestBean.getMysqlPassword, "MysqlPassword cannot be null")
    ValidationUtils.notblank(requestBean.getMysqlUsername, "MysqlPassword cannot be blank")
    ValidationUtils.notNull(requestBean.getSyncTaskId, "SyncTaskId cannot be null")
    ValidationUtils.notblank(requestBean.getSyncTaskId, "SyncTaskId cannot be null")
    ValidationUtils.notNull(requestBean.getZookeeperServers, "ZookeeperServers cannot be null")
    ValidationUtils.notblank(requestBean.getZookeeperServers, "ZookeeperServers cannot be blank")
    ValidationUtils.notZero(requestBean.getTaskType,"TaskType cannot be zero")
  }
}
