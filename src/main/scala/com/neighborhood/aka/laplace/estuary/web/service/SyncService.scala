package com.neighborhood.aka.laplace.estuary.web.service

import java.util.concurrent.ConcurrentHashMap

import com.fasterxml.jackson.databind.ObjectMapper
import com.neighborhood.aka.laplace.estuary.core.akkaUtil.SyncDaemonCommand.{ExternalGetAllRunningTask, ExternalRestartCommand, ExternalStopCommand, ExternalSuspendTimedCommand}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.web.akkaUtil.ActorRefHolder
import com.neighborhood.aka.laplace.estuary.web.bean.TaskRequestBean

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.runtime.universe._
import scala.util.Try

/**
  * Created by john_liu on 2019/1/17.
  *
  * 同步任务Service的原型
  * 定义了任务相关的方法
  * 1.开始一个任务
  *
  * @todo 操作改为异步
  * @author neighborhood.aka.laplace
  */
trait SyncService[Bean <: TaskRequestBean] {
  implicit val timeout = akka.util.Timeout(3 seconds)
  protected lazy val objectMapper: ObjectMapper = new ObjectMapper
  private lazy val requestBeanMap: ConcurrentHashMap[String, TaskRequestBean]
  = new ConcurrentHashMap[String, TaskRequestBean]()

  protected def subTaskMark: String = "::"

  /**
    * 入海口3存任务的表名
    *
    */
  protected def saveTaskInfoTableName: String = "estuary3_task_info"

  /**
    * 发送挂起命令
    *
    * @param syncTaskId
    * @param ts
    * @return
    */
  def sendSuspendCommand(syncTaskId: String, ts: Long): Boolean = {
    val re = checkAllRunningTaskName.contains(syncTaskId)
    if (re) ActorRefHolder.syncDaemon ! ExternalSuspendTimedCommand(syncTaskId, ts)
    re
  }

  /**
    * 重启一个syncTask
    *
    * @param syncTaskId
    * @return 是否存在一个restart
    */
  def restartTask(syncTaskId: String): Boolean = {
    val re = checkAllRunningTaskName.contains(syncTaskId)
    if (re) ActorRefHolder.syncDaemon ! ExternalRestartCommand(syncTaskId)
    re
  }

  /**
    * 获取当前类型的运行任务名称
    *
    * @return 当前类型的运行任务名称
    */
  def checkRunningTask(implicit typeTag: TypeTag[Bean]): String = {
    val list: List[String] = checkRunningTaskInternal(typeTag)

    s"""
           {
           "runningTasks":[${list.map(x =>s""""$x"""").mkString(",")}]
        }
        """.stripMargin


  }

  /**
    * 检查数据同步量
    *
    * @param syncTaskId 同步任务Id
    * @param typeTag    Scala反射
    * @return 计数JSON
    */
  def checkLogCount(syncTaskId: String)(implicit typeTag: TypeTag[Bean]): String = {

    val map = if (syncTaskId.contains(subTaskMark)) checkLogCountInternal(syncTaskId)
    else if (checkRunningTaskInternal.contains(syncTaskId)) checkLogCountInternal(syncTaskId) else Map.empty[String, Long]

    if (map.isEmpty)
      s"""{"syncTaskId":"${syncTaskId}"}"""
    else
      s"""
       {
       "syncTaskId":"${syncTaskId}",
       "sinkCount":${map("sinkCount")},
       "batchCount":${map("batchCount")},
       "fetchCount":${map("fetchCount")},
       "fetchCountPerSecond":${map("fetchCountPerSecond")},
       "batchCountPerSecond":${map("batchCountPerSecond")},
       "sinkCountPerSecond":${map("sinkCountPerSecond")}
       }
     """.stripMargin


  }

  def checkLogCost(syncTaskId: String)(implicit typeTag: TypeTag[Bean]): String = {
    val map = if (syncTaskId.contains(subTaskMark)) checkLogCostInternal(syncTaskId)
    else if (checkRunningTaskInternal.contains(syncTaskId)) checkLogCostInternal(syncTaskId) else Map.empty[String, Long]

    if (map.isEmpty)
      s"""{"syncTaskId":"${syncTaskId}"}"""
    else
      s"""
       {
        "syncTaskId":"${syncTaskId}",
        "fetchCost":${map("fetchCost")},
        "batchCost":${map("batchCost")},
        "sinkCost":${map("sinkCost")},
        "fetchCostPercentage":${map("fetchCostPercentage")},
        "batchCostPercentage":${map("batchCostPercentage")},
        "sinkCostPercentage":${map("sinkCostPercentage")}
       }
     """.stripMargin
  }

  /**
    * 检查savePoint
    *
    * @param syncTaskId
    * @return
    */
  def checkLastSavedLogPosition(syncTaskId: String)(implicit typeTag: TypeTag[Bean]): String = {
    if (syncTaskId.contains(subTaskMark)) checkLastSavedLogPositionInternal(syncTaskId) else if (checkRunningTaskInternal.contains(syncTaskId)) checkLastSavedLogPositionInternal(syncTaskId) else s"""{"syncTaskId":"$syncTaskId"}"""

  }

  /**
    * 开始一个同步任务并记录taskRequestBean
    *
    * @note 请调用这个方法启动！
    * @param syncTaskId      同步任务Id
    * @param taskRequestBean taskRequestBean
    * @return 启动信息
    */
  def startNewOneTaskKeepConfig(syncTaskId: String, taskRequestBean: Bean): String = {
    val re = if (checkAllRunningTaskName.contains(syncTaskId))
      s"""
        {
         "syncTaskId":"$syncTaskId"
         "status":"existed"
        }
      """.stripMargin
    else startNewOneTask(taskRequestBean)
    requestBeanMap.put(syncTaskId, taskRequestBean)
    re
  }

  /**
    * 停止一个已经开始的同步任务并移除Config
    *
    * @param syncTaskId 同步任务Id
    * @param typeTag    反射类型标签
    * @return false 没有这个任务 true 发送成功
    */
  def stopSyncTaskAndRomoveConfig(syncTaskId: String)(implicit typeTag: TypeTag[Bean]): Boolean = {
    val re = stopSyncTask(syncTaskId)(typeTag)
    requestBeanMap.remove(syncTaskId)
    s"""
      {
       "syncTaskId":"$syncTaskId",
       "result":$re
      }
    """.stripMargin
    re
  }

  /**
    * 查看任务的config信息
    *
    * @param syncTaskId
    * @return
    */
  def getTaskInfoConfig(syncTaskId: String): String = getTaskInfoConfigInternal(syncTaskId).map(x => objectMapper.writeValueAsString(x)).getOrElse("")

  /**
    * 检查任务运行状态
    *
    * @param syncTaskId 同步任务Id
    * @return 任务运行信息
    */
  def checkTaskStatus(syncTaskId: String): String = {
    import scala.collection.JavaConverters._
    val idStringKv = "syncTaskId" -> s"""$syncTaskId"""
    TaskManager.getTaskManager(syncTaskId)
    match {
      case Some(x) => {
        s"""
            {
            "status":${objectMapper.writeValueAsString(x.taskStatus.mapValues(_.toString).asJava)},
            $idStringKv
            }""".stripMargin
      }
      case None =>
        s"""
               {
              $idStringKv
               }
             """.stripMargin
    }
  }

  /**
    * 开始一个同步任务
    *
    * @param taskRequestBean 同步任务开始标志
    * @return 任务启动信息
    */
  protected def startNewOneTask(taskRequestBean: Bean): String

  /**
    * 停止一个同步任务
    *
    * @param syncTaskId
    * @return
    */
  protected def stopSyncTask(syncTaskId: String)(implicit typeTag: TypeTag[Bean]): Boolean = {
    val re = checkRunningTaskInternal(typeTag).contains(syncTaskId)
    if (re) ActorRefHolder.syncDaemon ! ExternalStopCommand(syncTaskId)
    re
  }

  /**
    * 获取全部运行任务的名称
    *
    * @return all name list
    */
  protected def checkAllRunningTaskName: List[String] = {
    import akka.pattern.ask
    Try(Await
      .result(ActorRefHolder.syncDaemon ? ExternalGetAllRunningTask, 3 seconds)
      .asInstanceOf[List[String]])
      .getOrElse(List.empty)
  }

  /**
    * 根据Bean类型过滤出属于当前Service的任务
    *
    * @param typeTag 类型标签
    * @return list
    */
  protected def checkRunningTaskInternal(implicit typeTag: TypeTag[Bean]): List[String] = checkAllRunningTaskName
    .filter(x => Option(requestBeanMap.get(x))
      .map(y => y.getClass.getCanonicalName == typeTag.tpe.toString) ////这个比较方式有点蠢
      .getOrElse(false))

  /**
    * 查看同步任务的同步的count数据
    *
    * @param syncTaskId
    * @return
    */
  protected def checkLogCountInternal(syncTaskId: String): Map[String, Long] = {
    TaskManager.getTaskManager(syncTaskId).map(_.logCount).getOrElse(Map.empty[String, Long])
  }

  /**
    * 查看同步任务的同步的cost数据
    *
    * @param syncTaskId
    * @return
    */
  protected def checkLogCostInternal(syncTaskId: String): Map[String, Long] = {
    TaskManager.getTaskManager(syncTaskId).map(_.logTimeCost).getOrElse(Map.empty[String, Long])
  }

  /**
    * 查看同步任务的同步的ScaePoint
    *
    * @param syncTaskId
    * @return
    */
  protected def checkLastSavedLogPositionInternal(syncTaskId: String): String = TaskManager.getTaskManager(syncTaskId).map(_.sinkerLogPosition.get()).getOrElse("")

  /**
    * 查看任务的config信息
    *
    * @param syncTaskId
    * @return
    */
  protected def getTaskInfoConfigInternal(syncTaskId: String): Option[Bean] = Option(requestBeanMap.get(syncTaskId))
    .flatMap(bean => Try(bean.asInstanceOf[Bean]).toOption)

  /**
    * 存任务信息
    *
    * @param syncTaskId 同步任务Id
    * @param ip         ip
    * @param port       端口
    * @param bean       任务信息bean
    * @param taskType   任务类型
    * @param f          存任务信息的方法 sql => 存
    */
  protected def saveTaskInfo(syncTaskId: String, ip: String, port: String, bean: Bean, taskType: String, databaseName: String = " ")(f: String => Unit) = {
    val sql = s"insert into $saveTaskInfoTableName(sync_task_id,ip,port,bean,task_type,db_name) VALUES('$syncTaskId','$ip','$port','${objectMapper.writeValueAsString(bean)}','$taskType','$databaseName')"
    f(sql)
  }
}
