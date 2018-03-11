package com.neighborhood.aka.laplace.estuary.actor

import akka.actor.{ActorRef, Props}
import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.core.akka.{SyncDaemon, theActorSystem}
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager
import com.neighborhood.aka.laplce.estuary.mysql.lifecycle.MysqlBinlogController
import com.typesafe.config.Config

/**
  * Created by john_liu on 2018/2/10.
  */
object Application extends theActorSystem {
  val daemon = this.system.actorOf(Props(classOf[SyncDaemon]),"syncDaemon")
  def main(args: Array[String]): Unit = {

//    val confPath = "/Users/john_liu/IdeaProjects/estuary/src/main/resources/application.conf"
//    val conf = new File(confPath)
//    val taskInfoBean = new Mysql2KafkaTaskInfoBean
//    val mysqlBean = new MysqlCredentialBean("10.10.177.227",3306,"admin","ukaOg4022VPb0E4vyQoT","")
//    taskInfoBean.master = mysqlBean
//    val config = ConfigFactory.load(ConfigFactory.parseFile(conf))

    val a = MysqlBinlogController.props(TestContext.config,TestContext.mysql2KafkaTaskInfoBean)


    daemon ! (a,Option("test1"))

  }





}
