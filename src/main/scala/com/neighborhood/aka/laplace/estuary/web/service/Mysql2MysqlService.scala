package com.neighborhood.aka.laplace.estuary.web.service


import java.net.InetAddress

import com.neighborhood.aka.laplace.estuary.core.akkaUtil.SyncDaemonCommand.ExternalStartCommand
import com.neighborhood.aka.laplace.estuary.core.task.Mysql2MysqlSyncTask
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.control.{MysqlBinlogInOrderController, MysqlBinlogInOrderMysqlController}
import com.neighborhood.aka.laplace.estuary.web.akkaUtil.ActorRefHolder
import com.neighborhood.aka.laplace.estuary.web.bean.{Mysql2MysqlRequestBean, SdaRequestBean, TableNameMappingBean}
import com.neighborhood.aka.laplace.estuary.web.utils.TaskBeanTransformUtil
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.beans.factory.annotation.{Autowired, Qualifier, Value}
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate

import scala.collection.JavaConverters._

/**
  * Created by john_liu on 2018/3/10.
  *
  * @author neighborhood.aka.laplace
  */
@Service("mysql2Mysql")
final class Mysql2MysqlService extends SyncService[Mysql2MysqlRequestBean] {
  @Value("spring.config.concerned.tableName")
  private val concernedConfigTableName: String = null
  @Value("server.port")
  private val port: String = null
  @Value("sda.tableMapping.matadata.url")
  private val mataDataUrl: String = null
  @Value("sda.tableMapping.matadata.token")
  private val mataDataToken: String = null
  @Autowired
  @Qualifier("configJdbcTemplate") private lazy val jdbcTemplate: JdbcTemplate = null
  private lazy val logger: Logger = LoggerFactory.getLogger(classOf[Mysql2MysqlService])

  @Autowired
  @Qualifier("restTemplate")
  private val restTemplate: RestTemplate = null

  /**
    * 为Sda定制的开始方法
    *
    * @param taskRequestBean 任务请求信息
    * @return 启动是否成功
    */
  def startNewOneTaskForSda(taskRequestBean: Mysql2MysqlRequestBean): String = {
    val syncTaskId = taskRequestBean.getMysql2MysqlRunningInfoBean.getSyncTaskId
    val ip = InetAddress.getLocalHost().getHostAddress
    val bean = taskRequestBean
    val taskType = "MYSQL_TO_MYSQL_SDA"
    val save: String => Unit = s => jdbcTemplate.update(s)
    customRequestForSda(taskRequestBean) //定制任务信息
    saveTaskInfo(syncTaskId, ip, port, bean, taskType)(save) //保存任务信息进入数据库
    startNewOneTaskKeepConfig(syncTaskId, taskRequestBean) //启动任务
  }

  /**
    * 开始一个同步任务
    *
    * @param taskRequestBean 同步任务开始标志
    * @return 任务启动信息
    */
  override protected def startNewOneTask(taskRequestBean: Mysql2MysqlRequestBean): String = {
    val taskInfoBean = TaskBeanTransformUtil.convertMysql2MysqlRequest2Mysql2MysqlTaskInfo(taskRequestBean)
    ActorRefHolder.syncDaemon ! ExternalStartCommand(Mysql2MysqlSyncTask(MysqlBinlogInOrderController.buildMysqlBinlogInOrderController(taskInfoBean, MysqlBinlogInOrderMysqlController.name), taskRequestBean.getMysql2MysqlRunningInfoBean.getSyncTaskId))
    s"""
      {
       "syncTaskId":"${taskRequestBean.getMysql2MysqlRunningInfoBean.getSyncTaskId}",
       "status":"submitted"
      }
    """.stripMargin
  }

  /**
    * 定制sda专用任务信息
    *
    * @param taskRequestBean 任务信息
    */
  private def customRequestForSda(taskRequestBean: Mysql2MysqlRequestBean): Unit = {
    val syncTaskId: String = taskRequestBean.getMysql2MysqlRunningInfoBean.getSyncTaskId
    val concernedTableNameSqlTemplete: String => String = x => s"select xxx from $concernedConfigTableName where  yyy = '$x'"
    val concernedDatabases: List[String] = taskRequestBean.getMysqlSourceBean.getConcernedDatabase.asScala.toList
    val getMappingRule: java.util.Map[String, String] = getAllTableMappingByDatabase(concernedDatabases.toSet).asJava //必须要是java map
    val concernedFilterPattern: String = concernedDatabases.flatMap {
      databaseName =>
        jdbcTemplate
          .queryForMap(concernedTableNameSqlTemplete(databaseName))
          .get("xxx").toString
          .split(",")
          .map(tablename => if (tablename.contains('.')) tablename else s"$databaseName.$tablename")
    }.mkString(",")
    taskRequestBean.getMysqlSourceBean.setFilterPattern(concernedFilterPattern) //强制设置concernedPattern
    taskRequestBean.getMysql2MysqlRunningInfoBean.setMappingFormatName("sda") //强制Sda
    taskRequestBean.setSdaBean(new SdaRequestBean(getMappingRule)) //增加rule
  }

  /**
    * 获取全部表的映射关系
    *
    * @return
    */
  private def getAllTableMappingByDatabase(concernedDatabases: Set[String]): Map[String, String] = {
    val map = new java.util.HashMap[String, String]()
    map.put("token", mataDataToken)
    map.put("accept", "*/*")
    restTemplate
      .getForEntity(mataDataUrl, classOf[java.util.List[TableNameMappingBean]], map)
      .getBody
      .asScala
      .withFilter(x => concernedDatabases.contains(x.getSourceDb)) //过滤
      .map { x => (s"${x.getSourceDb.trim.toLowerCase}.${x.getSourceTable.toLowerCase}" -> s"${x.getNewDb}.${x.getNewTable}") } //全部转为小写
      .toMap
  }

}
