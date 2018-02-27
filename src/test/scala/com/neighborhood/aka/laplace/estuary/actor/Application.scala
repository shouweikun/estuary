package com.neighborhood.aka.laplace.estuary.actor

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.core.akka.theActorSystem
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager
import com.neighborhood.aka.laplce.estuary.mysql.lifecycle.MySqlBinlogController
import com.typesafe.config.Config

/**
  * Created by john_liu on 2018/2/10.
  */
object Application extends theActorSystem {

  def main(args: Array[String]): Unit = {

//    val confPath = "/Users/john_liu/IdeaProjects/estuary/src/main/resources/application.conf"
//    val conf = new File(confPath)
//    val taskInfoBean = new Mysql2KafkaTaskInfoBean
//    val mysqlBean = new MysqlCredentialBean("10.10.177.227",3306,"admin","ukaOg4022VPb0E4vyQoT","")
//    taskInfoBean.master = mysqlBean
//    val config = ConfigFactory.load(ConfigFactory.parseFile(conf))

    val mysql2KafkaTaskInfoManager = TestContext.mysql2KafkaTaskInfoManager
    val controller =  init(mysql2KafkaTaskInfoManager)
    controller ! "start"
  }


  def buildManager(config: Config, mysql2KafkaTaskInfoBean: Mysql2KafkaTaskInfoBean): Mysql2KafkaTaskInfoManager = {
    new Mysql2KafkaTaskInfoManager(config, mysql2KafkaTaskInfoBean)
  }

  def init(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager) :ActorRef= {
    val actorSystem = this.system
    actorSystem.actorOf(Props(classOf[MySqlBinlogController], mysql2KafkaTaskInfoManager))

  }


}
