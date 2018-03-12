package com.neighborhood.aka.laplce.estuary.web.service

import com.neighborhood.aka.laplce.estuary.bean.task.Mysql2KafkaTaskInfoBean
import com.neighborhood.aka.laplce.estuary.mysql.lifecycle.MysqlBinlogController
import com.neighborhood.aka.laplce.estuary.web.akka.ActorRefHolder
import akka.pattern._
import akka.util.Timeout
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Created by john_liu on 2018/3/10.
  */
object Mysql2KafkaService {


  def startOneTask(mysql2KafkaTaskInfoBean: Mysql2KafkaTaskInfoBean):String = {
    val prop = MysqlBinlogController.props(mysql2KafkaTaskInfoBean)
    ActorRefHolder.syncDaemon ! (prop, Option(mysql2KafkaTaskInfoBean.syncTaskId))
    //todo 持久化任务
   "mession submitted"
  }

  def checkTaskStatus(syncTaskId: String): String = {
    implicit val timeout = Timeout.apply(3 seconds)
    val map = ActorRefHolder.actorRefMap
    map
      .get(syncTaskId)
    match {
      case Some(x) => {
        var re = "{result:failure}"
        (x ? "status")
          .onComplete {
            case Success(str) => re = s"{result:$str}"
            case Failure(e) => re = s"{$re,reason:$e}"
          }
        re
      }
      case None => "{result:failure,reason:\"不存在这个actor\"}"
    }
  }
}
