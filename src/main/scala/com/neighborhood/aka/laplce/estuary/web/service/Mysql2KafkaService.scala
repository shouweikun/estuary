package com.neighborhood.aka.laplce.estuary.web.service

import java.io.File

import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.mysql.lifecycle.MysqlBinlogController
import com.neighborhood.aka.laplce.estuary.web.akka.ActorRefHolder
import com.typesafe.config.ConfigFactory

/**
  * Created by john_liu on 2018/3/10.
  */
object Mysql2KafkaService {

  var configPath: String = "/Users/john_liu/IdeaProjects/estuary/src/main/resources/application.conf"
  val config = ConfigFactory.load(ConfigFactory.parseFile(new File(configPath)))

  def startOneTask(mysql2KafkaTaskInfoBean: Mysql2KafkaTaskInfoBean) = {
     val prop = MysqlBinlogController.props(config,mysql2KafkaTaskInfoBean)
     ActorRefHolder.syncDaemon ! (prop,Option(mysql2KafkaTaskInfoBean.syncTaskId))
   //todo 持久化任务

  }
}
