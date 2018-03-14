package com.neighborhood.aka.laplce.estuary.web.service

import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.mysql.Mysql2KafkaTaskInfoManager
import com.neighborhood.aka.laplce.estuary.mysql.lifecycle.MysqlBinlogController
import com.neighborhood.aka.laplce.estuary.web.akka.ActorRefHolder

import scala.util.parsing.json.JSON

/**
  * Created by john_liu on 2018/3/10.
  */
object Mysql2KafkaService {


  def startOneTask(mysql2KafkaTaskInfoBean: Mysql2KafkaTaskInfoBean): String = {
    val prop = MysqlBinlogController.props(mysql2KafkaTaskInfoBean)
    ActorRefHolder.syncDaemon ! (prop, Option(mysql2KafkaTaskInfoBean.syncTaskId))
    //todo 持久化任务
    "mession submitted"
  }

  def checkTaskStatus(syncTaskId: String): String = {
    Option(Mysql2KafkaTaskInfoManager.taskStatusMap.get(syncTaskId))
    match {
      case Some(x) => {
        s"{$syncTaskId:${x.map(kv => s"${kv._1}:${kv._2}").mkString(",")}}"
      }
      case None => s"$syncTaskId:None}"
    }

  }

  def reStartTask(syncTaskId: String): Boolean = {
    val map = ActorRefHolder.actorRefMap
    map
      .get(syncTaskId)
    match {
      case Some(x) => x ! "restart"; true
      case None => false
    }
  }

  def stopTask(syncTaskId: String): Boolean = {
    val map = ActorRefHolder.actorRefMap
    map
      .get(syncTaskId)
    match {
      case Some(x) => ActorRefHolder.system.stop(x); true
      case None => false
    }

  }

  def checkSystemStatus = {
   ???
  }
}
