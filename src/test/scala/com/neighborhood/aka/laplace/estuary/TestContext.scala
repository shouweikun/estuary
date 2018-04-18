package com.neighborhood.aka.laplace.estuary

import java.io.File

import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplace.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplace.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplace.estuary.mysql.JsonUtil
import com.typesafe.config.ConfigFactory
import org.scalamock.scalatest.MockFactory

import scala.util.parsing.json.JSON

/**
  * Created by john_liu on 2018/2/18.
  */
object TestContext extends MockFactory{
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
  val kafkaConfigJson = JsonUtil.Json2JavaMap("{\n      \"bootstrap.servers\": \"10.10.248.207:6667;10.10.237.78:6667;10.10.219.186:6667\",\n      \"max.block.ms\": 3000,\n      \"max.request.size\": 10485760,\n      \"request.timeout.ms\": 30000,\n      \"acks\": \"1\",\n      \"linger.ms\": 0,\n      \"retries\": 10\n    }")
  val topic: String = "test"
  /**
    * Mysql2KafkaBean
    */
  val mysql2KafkaTaskInfoBean = new Mysql2KafkaTaskInfoBean
  mysql2KafkaTaskInfoBean.master = mysqlBean
  mysql2KafkaTaskInfoBean.topic = topic

  /**
    * StartPosition
    */
  val startPosition = new EntryPosition("mysql-bin.000013", 4L)


  def buildMysql2KafkaTaskManager = {

  }

  def isJson(str: String): Boolean = {
    JSON.parseRaw(str).fold(false)(x => true)
  }

}
