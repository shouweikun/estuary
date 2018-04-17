package com.neighborhood.aka.laplace.estuary.web.service

import com.neighborhood.aka.laplace.estuary.bean.support.MysqlTaskBeanTransform
import com.neighborhood.aka.laplace.estuary.mongodb.MongoPersistence
import com.neighborhood.aka.laplace.estuary.mysql.Mysql2KafkaTaskInfoManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.MysqlBinlogController
import com.neighborhood.aka.laplace.estuary.web.akka.ActorRefHolder
import com.neighborhood.aka.laplace.estuary.web.akka.ActorRefHolder.actorRefMap
import com.neighborhood.aka.laplace.estuary.web.bean.Mysql2kafkaTaskRequestBean
import org.slf4j.{Logger, LoggerFactory}



/**
  * Created by john_liu on 2018/3/10.
  */
object Mysql2KafkaService {

  val logger:Logger = LoggerFactory.getLogger(Mysql2KafkaService.getClass)
  val mongoPersistence=new MongoPersistence
  val mysqlTaskBeanTransform=new MysqlTaskBeanTransform

  // 根据syncTaskId从mongodb中查询出详细信息
  def loadOneExistTask(key: String,value: String):Mysql2kafkaTaskRequestBean = {
    val mongoValue=mongoPersistence.getKV(classOf[Mysql2kafkaTaskRequestBean],key,value)
    mongoValue
  }

  // 从mongodb中查出所有的mysql的同步任务
  def loadAllExistTask: java.util.List[Mysql2kafkaTaskRequestBean] = {
    mongoPersistence.findAll(classOf[Mysql2kafkaTaskRequestBean])
  }

  def startAllExistTask: String = {
    import scala.collection.JavaConversions._
        loadAllExistTask
          .map(startAllExistTasks(_))
          .mkString(",")

  }
  def startOneExistTask(key: String,value: String): String = {
    Option(actorRefMap.get(key))
    match{
      case value => {
        s"mession: exist task ${value} already started"
      }
      case _ => {
        val mysql2kafkaTaskRequestBean=loadOneExistTask(key,value)
        val mysql2KafkaTaskInfoBean= mysqlTaskBeanTransform.transform(mysql2kafkaTaskRequestBean)
        val prop = MysqlBinlogController.props(mysql2KafkaTaskInfoBean)
        ActorRefHolder.syncDaemon ! (prop, Option(mysql2KafkaTaskInfoBean.syncTaskId))
        //    开启已经存在的任务不需要持久化
        s"mession: exist task ${mysql2KafkaTaskInfoBean.syncTaskId} submitted"
      }
    }
  }

  def startAllExistTasks(mysql2kafkaTaskRequestBean: Mysql2kafkaTaskRequestBean):String={
    val mysql2KafkaTaskInfoBean= mysqlTaskBeanTransform.transform(mysql2kafkaTaskRequestBean)
    val prop = MysqlBinlogController.props(mysql2KafkaTaskInfoBean)
    ActorRefHolder.syncDaemon ! (prop, Option(mysql2KafkaTaskInfoBean.syncTaskId))
    //    开启已经存在的任务不需要持久化
    s"mession:${mysql2KafkaTaskInfoBean.syncTaskId} submitted"
  }

  def startNewOneTask(mysql2kafkaTaskRequestBean: Mysql2kafkaTaskRequestBean): String = {
    val mysql2KafkaTaskInfoBean= mysqlTaskBeanTransform.transform(mysql2kafkaTaskRequestBean)
    val prop = MysqlBinlogController.props(mysql2KafkaTaskInfoBean)
    ActorRefHolder.syncDaemon ! (prop, Option(mysql2KafkaTaskInfoBean.syncTaskId))
    //todo 持久化任务
    mongoPersistence.save(mysql2kafkaTaskRequestBean)
    s"mession:${mysql2KafkaTaskInfoBean.syncTaskId} submitted"
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
    Option(map
      .get(syncTaskId))
    match {
      case Some(x) => x ! "restart"; true
      case None => false
    }
  }

  def stopTask(syncTaskId: String): Boolean = {
    val map = ActorRefHolder.actorRefMap
    Option(
      map
        .get(syncTaskId)
    ) match {
      case Some(x) => ActorRefHolder.system.stop(x); map.remove(syncTaskId); true
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
      case Some(x) => if (x.taskInfo.isCounting) s"{$syncTaskId: ${
        Mysql2KafkaTaskInfoManager
          .logCount(x)
          .map(kv => s"${kv._1}:${kv._2}")
          .mkString(",")
      } }" else s"{$syncTaskId:count is not set}"
      case None => "task not exist"
    }
  }

  def checkTimeCost(syncTaskId: String): String = {
    val manager = Mysql2KafkaTaskInfoManager.taskManagerMap.get(syncTaskId)
    Option(manager)
    match {
      case Some(x) => if (x.taskInfo.isCosting) s"{$syncTaskId: ${
        Mysql2KafkaTaskInfoManager
          .logTimeCost(x)
          .map(kv => s"${kv._1}:${kv._2}")
          .mkString(",")
      } }" else s"{$syncTaskId:profiling is not set}"
      case None => "task not exist"
    }
  }

  def checklastSavedlogPosition(syncTaskId: String): String = {
    val manager = Mysql2KafkaTaskInfoManager.taskManagerMap.get(syncTaskId)
    Option(manager)
    match {
      case Some(x) => if (x.taskInfo.isProfiling) s"{$syncTaskId:${x.sinkerLogPosition.get()} }" else s"{$syncTaskId:profiling is not set}"
      case None => "task not exist"
    }
  }
}
