package com.neighborhood.aka.laplace.estuary.web.service

import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.MysqlBinlogController
import com.neighborhood.aka.laplace.estuary.mysql.task.Mysql2KafkaTaskInfoManager
import com.neighborhood.aka.laplace.estuary.web.akkaUtil.ActorRefHolder
import com.neighborhood.aka.laplace.estuary.web.akkaUtil.ActorRefHolder.actorRefMap
import com.neighborhood.aka.laplace.estuary.web.bean.Mysql2kafkaTaskRequestBean
import com.neighborhood.aka.laplace.estuary.web.dao.MongoPersistence
import com.neighborhood.aka.laplace.estuary.web.utils.TaskBeanTransformUtil
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.Autowired


/**
  * Created by john_liu on 2018/3/10.
  */
object Mysql2KafkaService {

  val logger: Logger = LoggerFactory.getLogger(Mysql2KafkaService.getClass)

  val mongoPersistence = new MongoPersistence[Mysql2kafkaTaskRequestBean]

  // 根据syncTaskId从mongodb中查询出详细信息
  def loadOneExistTask(key: String, value: String): Mysql2kafkaTaskRequestBean = {
    val mongoValue = mongoPersistence.getKV(classOf[Mysql2kafkaTaskRequestBean], key, value)
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

  def startOneExistTask(key: String, value: String): String = {
    Option(actorRefMap.get(key)).fold {
      val mysql2kafkaTaskRequestBean = loadOneExistTask(key, value)
      val mysql2KafkaTaskInfoBean = TaskBeanTransformUtil.transform2Mysql2KafkaTaskInfoBean(mysql2kafkaTaskRequestBean)
      val prop = MysqlBinlogController.props(mysql2KafkaTaskInfoBean)
      ActorRefHolder.syncDaemon ! (prop, Option(mysql2KafkaTaskInfoBean.syncTaskId))
      //    开启已经存在的任务不需要持久化
      s"mession: exist task ${mysql2KafkaTaskInfoBean.syncTaskId} submitted"
    }(value => s"mession: exist task ${value} already started")
  }

  def startAllExistTasks(mysql2kafkaTaskRequestBean: Mysql2kafkaTaskRequestBean): String = {
    val mysql2KafkaTaskInfoBean = TaskBeanTransformUtil.transform2Mysql2KafkaTaskInfoBean(mysql2kafkaTaskRequestBean)
    val prop = MysqlBinlogController.props(mysql2KafkaTaskInfoBean)
    ActorRefHolder.syncDaemon ! (prop, Option(mysql2KafkaTaskInfoBean.syncTaskId))
    //    开启已经存在的任务不需要持久化
    s"mession:${mysql2KafkaTaskInfoBean.syncTaskId} submitted"
  }

  def startNewOneTask(mysql2kafkaTaskRequestBean: Mysql2kafkaTaskRequestBean): String = {
    val mysql2KafkaTaskInfoBean = TaskBeanTransformUtil.transform2Mysql2KafkaTaskInfoBean(
      mysql2kafkaTaskRequestBean)
    val prop = MysqlBinlogController.props(mysql2KafkaTaskInfoBean)
    ActorRefHolder.syncDaemon ! (prop, Option(mysql2KafkaTaskInfoBean.syncTaskId))
    //todo 持久化任务
   // mongoPersistence.save(mysql2kafkaTaskRequestBean)
    s"mession:${mysql2KafkaTaskInfoBean.syncTaskId} submitted"
  }

  def checkRunningTaskIds: String = {
    import scala.collection.JavaConverters._
    s"{runningTasks:[${
      ActorRefHolder
        .actorRefMap
        .asScala
        .map(kv => s""""${kv._1}"""")
        .mkString(",")
    }]}"
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
