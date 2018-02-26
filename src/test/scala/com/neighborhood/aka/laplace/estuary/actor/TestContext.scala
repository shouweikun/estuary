package com.neighborhood.aka.laplace.estuary.actor

import java.io.File
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplce.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.core.utils.JsonUtil
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager
import com.typesafe.config.ConfigFactory

/**
  * Created by john_liu on 2018/2/18.
  */
object TestContext {
  /**
    * Config
    */
  val dummyMysqlEventParser = buildDummyMysqlEventParser
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
  val mysqlBean = new MysqlCredentialBean(mysqlAddress, mysqlPort, mysqlUsrname, mysqlPassword, mysqlDefaultDatabase)
  /**
    * Kafka基本信息
    */
  val kafkaConfigJson = JsonUtil.Json2JavaMap("{\n      \"bootstrap.servers\": \"10.10.71.76:9092;10.10.71.14:9092;10.10.72.234:9092\",\n      \"max.block.ms\": 3000,\n      \"max.request.size\": 10485760,\n      \"request.timeout.ms\": 8000,\n      \"acks\": \"1\",\n      \"linger.ms\": 0,\n      \"retries\": 10\n    }")
  val topic: String = "test"
  /**
    * Mysql2KafkaBean
    */
  val mysql2KafkaTaskInfoBean = new Mysql2KafkaTaskInfoBean
  mysql2KafkaTaskInfoBean.master = mysqlBean
  mysql2KafkaTaskInfoBean.topic = topic
  mysql2KafkaTaskInfoBean.configMapFromJson = kafkaConfigJson
  /**
    * StartPosition
    */
  val startPosition = new EntryPosition("mysql-bin.000040", 4L)
  /**
    * Mysql2KafkaManager
    */
  val mysql2KafkaTaskInfoManager = new Mysql2KafkaTaskInfoManager(config, mysql2KafkaTaskInfoBean)
  mysql2KafkaTaskInfoManager.startPosition = startPosition


  def buildDummyMysqlEventParser: Class[_] = Class.forName("com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser")
}
