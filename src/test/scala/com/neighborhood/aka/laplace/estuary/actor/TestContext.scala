package com.neighborhood.aka.laplace.estuary.actor

import java.io.File

import com.neighborhood.aka.laplce.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager
import com.typesafe.config.ConfigFactory

/**
  * Created by john_liu on 2018/2/18.
  */
object TestContext {
  /**
    * Config
    */
  val confPath = "/Users/john_liu/IdeaProjects/estuary/src/main/resources/application.conf"
  val conf = new File(confPath)
  val config = ConfigFactory.load(ConfigFactory.parseFile(conf))
  /**
    * Mysql基本信息
    */
  val mysqlAddress = "10.10.177.227"
  val mysqlPort = 3306
  val mysqlUsrname = "admin"
  val mysqlPassword = "ukaOg4022VPb0E4vyQoT"
  val mysqlDefaultDatabase = ""
  val mysqlBean = new MysqlCredentialBean(mysqlAddress,mysqlPort,mysqlUsrname,mysqlPassword,mysqlDefaultDatabase)
  /**
    * Kafka基本信息
    */
  val brokerList: String = "10.10.71.76:9092,10.10.71.14:9092,10.10.72.234:9092"
  val serializerClass: String = "kafka.serializer.StringEncoder"
  val partitionerClass: String = "com.kafka.myparitioner.CidPartitioner"
  val requiredAcks: String = "1"
  val topic:String = "test"
  /**
    * Mysql2KafkaBean
    */
  val mysql2KafkaTaskInfoBean = new Mysql2KafkaTaskInfoBean
  mysql2KafkaTaskInfoBean.master = mysqlBean
  mysql2KafkaTaskInfoBean.brokerList = brokerList
  mysql2KafkaTaskInfoBean.serializerClass = serializerClass
  mysql2KafkaTaskInfoBean.requiredAcks = requiredAcks
  mysql2KafkaTaskInfoBean.topic = topic
  /**
    * Mysql2KafkaManager
    */
  val mysql2KafkaTaskInfoManager = new Mysql2KafkaTaskInfoManager(config,mysql2KafkaTaskInfoBean)
}
