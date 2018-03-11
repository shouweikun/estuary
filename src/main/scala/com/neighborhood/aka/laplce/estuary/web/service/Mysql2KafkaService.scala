package com.neighborhood.aka.laplce.estuary.web.service

import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.mysql.lifecycle.MysqlBinlogController
import com.neighborhood.aka.laplce.estuary.web.akka.ActorRefHolder
import akka.pattern._
import akka.util.Timeout
import scala.concurrent.duration._

/**
  * Created by john_liu on 2018/3/10.
  */
object Mysql2KafkaService {


  def startOneTask(mysql2KafkaTaskInfoBean: Mysql2KafkaTaskInfoBean) = {
    val prop = MysqlBinlogController.props(mysql2KafkaTaskInfoBean)
    ActorRefHolder.syncDaemon ! (prop, Option(mysql2KafkaTaskInfoBean.syncTaskId))
    //todo 持久化任务

  }

  def checkTaskStatus(syncTaskId: String): String = {
    implicit val timeout = Timeout.apply(3 seconds)
    ActorRefHolder
      .actorRefMap.get(syncTaskId)
    match {
      case Some(x) => x
        .ask("status")
        .value
        .get
        .getOrElse("result:failure")
        .toString
      case None => "result:failure"
    }
  }
}
