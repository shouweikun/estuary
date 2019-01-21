package com.neighborhood.aka.laplace.estuary.web.service

import java.util.concurrent.ConcurrentHashMap

import com.fasterxml.jackson.databind.ObjectMapper
import com.neighborhood.aka.laplace.estuary.web.akkaUtil.ActorRefHolder
import com.neighborhood.aka.laplace.estuary.web.bean.{Mysql2kafkaTaskRequestBean, SnapshotRequestBean}
import org.slf4j.{Logger, LoggerFactory}


/**
  * Created by john_liu on 2018/3/10.
  */
object Mysql2KafkaService {

  private lazy val logger: Logger = LoggerFactory.getLogger(Mysql2KafkaService.getClass)
  private lazy val objectMapper: ObjectMapper = new ObjectMapper
  private lazy val requestBeanMap: ConcurrentHashMap[String, Mysql2kafkaTaskRequestBean]
  = new ConcurrentHashMap[String, Mysql2kafkaTaskRequestBean]()

  def checkSnapshotTaskRunningInfo(syncTaskId: String): String = {
    //    val manager = Mysql2KafkaTaskInfoManager.taskManagerMap.get(syncTaskId)
    //    Option(manager).fold {
    //      s"""{
    //            syncTaskId:"$syncTaskId"
    //          }""".stripMargin
    //    } {
    //      _.snapshotStauts.get()
    //    }
    ???
  }

  def startNewSnapshotTask(taskBean: SnapshotRequestBean): String = {
    //    lazy val syncTaskId = taskBean.getSyncTaskId
    //    val map = ActorRefHolder.actorRefMap
    //    Option(map.get(syncTaskId)).fold(
    //      s"""
    //        {
    //         syncTaskId:"$syncTaskId"
    //         submit:false
    //        }
    //      """.stripMargin) {
    //      ref =>
    //        ref ! TaskBeanTransformUtil.convert2SnapshotTask(taskBean)
    //        s"""
    //        {
    //         syncTaskId:"$syncTaskId"
    //         submit:true
    //        }
    //      """.stripMargin
    //    }
    ???
  }

  /**
    * 任务类型对应：
    *          1. mysql2kafka
    *          2. mysql2kafkaInOrder
    *
    * @param mysql2kafkaTaskRequestBean
    * @return
    */
  def startNewOneTask(mysql2kafkaTaskRequestBean: Mysql2kafkaTaskRequestBean): String = {
    //    val mysql2KafkaTaskInfoBean = TaskBeanTransformUtil.transform2Mysql2KafkaTaskInfoBean(
    //      mysql2kafkaTaskRequestBean)
    //    val taskType = mysql2kafkaTaskRequestBean.getTaskType
    //    lazy val prop = {
    //      taskType match {
    //        case 1 => MysqlBinlogController.props(mysql2KafkaTaskInfoBean)
    //        case 2 => MysqlBinlogInOrderController.props(mysql2KafkaTaskInfoBean)
    //      }
    //
    //    }
    //    taskType match {
    //      case 1 => ActorRefHolder.syncDaemon ! Mysql2KafkaTask(prop, Option(mysql2KafkaTaskInfoBean.syncTaskId))
    //      case 2 => ActorRefHolder.syncDaemon ! Mysql2KafkaInOrderTask(prop, Option(mysql2KafkaTaskInfoBean.syncTaskId))
    //      case x => throw new IllegalArgumentException(s"不支持的任务类型,$x,id:${Option(mysql2KafkaTaskInfoBean.syncTaskId).getOrElse("None,pls do not let it none")}")
    //    }
    //    requestBeanMap.put(mysql2KafkaTaskInfoBean.syncTaskId, mysql2kafkaTaskRequestBean)
    //    s"mession:${mysql2KafkaTaskInfoBean.syncTaskId} submitted"
    ???
  }

  def getEstuaryConfigString(syncTaskId: String): String = Option(requestBeanMap.get(syncTaskId))
    .fold(s"""{"syncTaskId":"$syncTaskId"}""")(objectMapper.writeValueAsString(_))


  def checkRunningTaskIds: String = {
    import scala.collection.JavaConverters._
    s"""
       {
       runningTasks:[${
      ActorRefHolder
        .actorRefMap
        .asScala
        .map(kv => s""""${kv._1}"""")
        .mkString(",\n")
    }]
    }
     """.stripMargin
  }

  def checkTaskStatus(syncTaskId: String): String = {
    //    val idStringKv = ("syncTaskId" -> s"""$syncTaskId""")
    //    Option(Mysql2KafkaTaskInfoManager.taskStatusMap.get(syncTaskId))
    //    match {
    //      case Some(x) => {
    //        s"""
    //        {
    //           ${x.+(idStringKv).map(kv => s""""${kv._1}":"${kv._2}"""").mkString(",\n")}
    //        }""".stripMargin
    //      }
    //      case None =>
    //        s"""
    //           {
    //           syncTaskId:"$syncTaskId"
    //           }
    //         """.stripMargin
    //    }
    ???
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
      case Some(x) => ActorRefHolder.system.stop(x); map.remove(syncTaskId); requestBeanMap.remove(syncTaskId); true
      case None => false
    }

  }

  def checklogCount(syncTaskId: String): String = {
    ???
    //    val manager = Mysql2KafkaTaskInfoManager.taskManagerMap.get(syncTaskId)
    //    Option(manager)
    //    match {
    //      case Some(x) => if (x.taskInfo.isCounting) Mysql2KafkaTaskInfoManager.logCount(x)
    //      else
    //        s"""
    //              {
    //              syncTaskId:"$syncTaskId"
    //              }
    //            """.stripMargin
    //      case None =>
    //        """
    //          {
    //          syncTaskId:"None"
    //          }
    //        """.stripMargin
    //    }
  }

  def checkTimeCost(syncTaskId: String): String = {
    ???
    //    val manager = Mysql2KafkaTaskInfoManager.taskManagerMap.get(syncTaskId)
    //    Option(manager)
    //    match {
    //      case Some(x) => if (x.taskInfo.isCosting) Mysql2KafkaTaskInfoManager.logTimeCost(x) else
    //        s"""
    //              {
    //          syncTaskId:"$syncTaskId"
    //              }
    //            """.stripMargin
    //      case None => "{syncTaskId:\"None\"}"
    //    }
  }

  def checkLastSavedLogPosition(syncTaskId: String): String = {
    ???
    //    val manager = Mysql2KafkaTaskInfoManager.taskManagerMap.get(syncTaskId)
    //    Option(manager)
    //    match {
    //      case Some(x) => if (x.taskInfo.isProfiling) x.sinkerLogPosition.get else
    //        s"""
    //           {
    //            syncTaskId:$syncTaskId
    //           }
    //         """.stripMargin
    //      case None => "{syncTaskId:None}"
    //    }
  }
}
