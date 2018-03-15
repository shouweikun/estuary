package com.neighborhood.aka.laplace.estuary.web.service

import com.neighborhood.aka.laplace.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplace.estuary.mysql.Mysql2KafkaTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.MysqlBinlogController
import com.neighborhood.aka.laplace.estuary.web.akka.ActorRefHolder

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
      case Some(x) => ActorRefHolder.system.stop(x); ActorRefHolder.actorRefMap.filter(!_._1.equals(syncTaskId)) ;true
      case None => false
    }

  }

  def checkSystemStatus = {
    ???
  }

  def checklogCount(syncTaskId: String): String = {
    val manager = Mysql2KafkaTaskInfoManager.taskManagerMap.get(syncTaskId)
    Option(manager)
    match {
      case Some(x) =>if(x.taskInfo.isCounting) s"{$syncTaskId: ${
        Mysql2KafkaTaskInfoManager
          .logCount(x)
          .map(kv => s"${kv._1}:${kv._2}")
          .mkString(",")
      } }" else s"{$syncTaskId:count is not set}"
      case None => "task not exist"
    }
  }

  def checkTimeCost(syncTaskId:String):String = {
    val manager = Mysql2KafkaTaskInfoManager.taskManagerMap.get(syncTaskId)
    Option(manager)
    match {
      case Some(x) =>if(x.taskInfo.isProfiling) s"{$syncTaskId: ${
        Mysql2KafkaTaskInfoManager
          .logTimeCost(x)
          .map(kv => s"${kv._1}:${kv._2}")
          .mkString(",")
      } }" else s"{$syncTaskId:profiling is not set}"
      case None => "task not exist"
    }
  }
}
