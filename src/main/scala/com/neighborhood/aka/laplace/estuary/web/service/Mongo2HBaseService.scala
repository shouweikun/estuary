package com.neighborhood.aka.laplace.estuary.web.service

import java.net.InetAddress

import com.neighborhood.aka.laplace.estuary.core.akkaUtil.SyncDaemonCommand.ExternalStartCommand
import com.neighborhood.aka.laplace.estuary.core.task.Mongo2HBaseSyncTask
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.control.hbase.{Oplog2HBaseController, Oplog2HBaseMultiInstanceController}
import com.neighborhood.aka.laplace.estuary.web.akkaUtil.ActorRefHolder
import com.neighborhood.aka.laplace.estuary.web.bean.Mongo2HBaseTaskRequestBean
import com.neighborhood.aka.laplace.estuary.web.utils.TaskBeanTransformUtil
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.{Autowired, Qualifier, Value}
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate

/**
  * Created by john_liu on 2019/3/15.
  *
  * @author neighborhood.aka.laplace
  */

@Service("mongo2hbase")
final class Mongo2HBaseService extends SyncService[Mongo2HBaseTaskRequestBean] {
  private lazy val logger: Logger = LoggerFactory.getLogger(classOf[Mongo2HBaseService])
  @Value("${server.port}")
  private val port: String = null
  @Autowired
  @Qualifier("configJdbcTemplate")
  private val jdbcTemplate: JdbcTemplate = null


  @Autowired
  @Qualifier("restTemplate")
  private val restTemplate: RestTemplate = null

  /**
    * 开始一个同步任务
    *
    * @param taskRequestBean 同步任务开始标志
    * @return 任务启动信息
    */
  override protected def startNewOneTask(taskRequestBean: Mongo2HBaseTaskRequestBean): String = {
    val taskInfoBean = TaskBeanTransformUtil.convertMongo2HBaseRequest2Mongo2HBaseTaskInfo(taskRequestBean)
    val props = if (taskRequestBean.isMulti) Oplog2HBaseMultiInstanceController.props(taskInfoBean) else Oplog2HBaseController.props(taskInfoBean)

    ActorRefHolder.syncDaemon ! ExternalStartCommand(Mongo2HBaseSyncTask(props, taskRequestBean.getMongo2HBaseRunningInfo.getSyncTaskId))
    s"""
      {
       "syncTaskId":"${taskRequestBean.getMongo2HBaseRunningInfo.getSyncTaskId}",
       "status":"submitted"
      }
    """.stripMargin
  }

  /**
    * 启动一个新的sda mongo2Hbase任务
    *
    * 1.sda定制
    * 2.保存任务信息
    * 3.启动
    *
    * @param taskRequestBean Mongo2HBaseTaskRequestBean
    * @return 任务启动情况
    */
  def startNewOneTask4Sda(taskRequestBean: Mongo2HBaseTaskRequestBean): String = {
    customRequest4Sda(taskRequestBean)
    val syncTaskId = taskRequestBean.getMongo2HBaseRunningInfo.getSyncTaskId
    val ip = InetAddress.getLocalHost().getHostAddress
    val bean = taskRequestBean
    val taskType = "MONGO_TO_HBASE_SDA"
    val database = taskRequestBean.getMongoSource.getConcernedNs.headOption.map(_.split('.')(0)).getOrElse("")
    val save: String => Unit = s => jdbcTemplate.update(s)
      saveTaskInfo(
        syncTaskId = syncTaskId,
        ip = ip,
        port = port,
        taskType = taskType,
        bean = bean,
        databaseName = database
      )(save)
    startNewOneTaskKeepConfig(taskRequestBean.getMongo2HBaseRunningInfo.getSyncTaskId, taskRequestBean)
  }

  /**
    * 定制sda 需求
    * 1.使用sda专用fetcher
    *
    * @param taskRequestBean Mongo2HBaseTaskRequestBean
    */
  private def customRequest4Sda(taskRequestBean: Mongo2HBaseTaskRequestBean): Unit = {
    if (taskRequestBean.getMongo2HBaseRunningInfo.getFetcherNameToLoad == null) taskRequestBean.getMongo2HBaseRunningInfo.setFetcherNameToLoad(new java.util.HashMap[String, String])
    taskRequestBean.getMongo2HBaseRunningInfo.getFetcherNameToLoad.put("directFetcher", "com.neighborhood.aka.laplace.estuary.mongo.lifecycle.fetch.SimpleOplogFetcher4sda")
  }
}
