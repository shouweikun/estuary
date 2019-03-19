package com.neighborhood.aka.laplace.estuary.web.service


import java.net.InetAddress
import java.util
import java.util.concurrent.atomic.AtomicBoolean

import com.neighborhood.aka.laplace.estuary.core.akkaUtil.SyncDaemonCommand.ExternalStartCommand
import com.neighborhood.aka.laplace.estuary.core.task.Mysql2MysqlSyncTask
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.control.{MysqlBinlogInOrderController, MysqlBinlogInOrderMysqlController}
import com.neighborhood.aka.laplace.estuary.mysql.task.mysql.Mysql2MysqlAllTaskInfoBean
import com.neighborhood.aka.laplace.estuary.web.akkaUtil.ActorRefHolder
import com.neighborhood.aka.laplace.estuary.web.bean.{Mysql2MysqlRequestBean, SdaRequestBean}
import com.neighborhood.aka.laplace.estuary.web.utils.TaskBeanTransformUtil
import org.springframework.beans.factory.annotation.{Autowired, Qualifier, Value}
import org.springframework.http.{HttpEntity, HttpHeaders, HttpMethod}
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.web.client.RestTemplate

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by john_liu on 2018/3/10.
  *
  * @author neighborhood.aka.laplace
  */
@Service("mysql2Mysql")
final class Mysql2MysqlService extends SyncService[Mysql2MysqlRequestBean] {

  @Value("${spring.config.concerned.tableName}")
  private val concernedConfigTableName: String = null
  @Value("${spring.config.encryptField.tableName}")
  private val encryptFieldInfoTableName: String = null
  @Value("${server.port}")
  private val port: String = null
  @Value("${sda.tableMapping.matadata.url}")
  private val mataDataUrl: String = null
  @Value("${sda.tableMapping.matadata.token}")
  private val mataDataToken: String = null
  @Autowired
  @Qualifier("configJdbcTemplate")
  private val jdbcTemplate: JdbcTemplate = null

  @Autowired
  @Qualifier("restTemplate")
  private val restTemplate: RestTemplate = null

  private val status: AtomicBoolean = new AtomicBoolean(false)

  def getNewTaskInfoBeanAndUpdate(syncTaskId: String): Option[Mysql2MysqlAllTaskInfoBean] = {
    Try(requestBeanMap.get(syncTaskId))
      .map(_.asInstanceOf[Mysql2MysqlRequestBean])
      .toOption
      .flatMap(Option(_))
      .map {
        bean =>
          customRequestForSda(bean)
          requestBeanMap.put(syncTaskId, bean)
          TaskBeanTransformUtil.convertMysql2MysqlRequest2Mysql2MysqlTaskInfo(bean)
      }
  }

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
    if (status.compareAndSet(false, true)) Mysql2MysqlService.ref = this //这是一个不好的实现
    val taskInfoBean = TaskBeanTransformUtil.convertMysql2MysqlRequest2Mysql2MysqlTaskInfo(taskRequestBean)
    val controllerName = taskInfoBean.taskRunningInfoBean.controllerNameToLoad.get("syncController").getOrElse(MysqlBinlogInOrderMysqlController.name)
    ActorRefHolder.syncDaemon ! ExternalStartCommand(Mysql2MysqlSyncTask(MysqlBinlogInOrderController.buildMysqlBinlogInOrderController(taskInfoBean, controllerName), taskRequestBean.getMysql2MysqlRunningInfoBean.getSyncTaskId))
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
    logger.info(s"start custom request 4 sda,$syncTaskId")
    val concernedTableNameSqlTemplate: String => String = x => s"select table_name from $concernedConfigTableName where  db_name = '$x' and db_type ='mysql' "
    val concernedDatabases: List[String] = taskRequestBean.getMysqlSourceBean.getConcernedDatabase.asScala.toList
    logger.info(s"we get concerned database:${concernedDatabases.mkString(",")},id:$syncTaskId")
    val getMappingRule: java.util.Map[String, String] = getAllTableMappingByDatabase(concernedDatabases.toSet).asJava //必须要是java map
    logger.info(s"we get table Mapping rule:${getMappingRule.asScala.mkString(",")},id:$syncTaskId")
    val concernedFilterPattern: String = concernedDatabases.flatMap {
      databaseName =>
        val sql = concernedTableNameSqlTemplate(databaseName)
        jdbcTemplate
          .queryForList(sql)
          .asScala
          .map(_.get("table_name").toString)
          .map(tableName => if (tableName.contains('.')) tableName.split('.')(1) else s"$tableName")
          .flatMap(x => List(s"$databaseName.$x", s"$databaseName._${x}_new", s"$databaseName._${x}_temp", s"$databaseName._${x}_old")) //增加临时表的白名单
          .toList
    }
      .mkString(",")
    logger.info(s"we get concerned filter pattern:$concernedFilterPattern,specially considering online ddl,and override input fitler pattern id:$syncTaskId")
    val allEncryptField = getAllEncryptField(concernedDatabases)
    logger.info(s"we get allEncryptField:${allEncryptField.asScala.mapValues(_.asScala).mkString(",")},id:$syncTaskId")
    taskRequestBean.getMysqlSourceBean.setFilterPattern(concernedFilterPattern) //强制设置concernedPattern
    logger.info(s"using sda mapping format,id:$syncTaskId")
    taskRequestBean.getMysql2MysqlRunningInfoBean.setMappingFormatName("sda") //强制Sda
    logger.info(s"using SdaMysqlBinlogInOrderDirectFetcher,id:$syncTaskId ")
    taskRequestBean.getMysql2MysqlRunningInfoBean.setFetcherNameToLoad(new util.HashMap[String, String]())
    taskRequestBean.getMysql2MysqlRunningInfoBean.getFetcherNameToLoad.put("directFetcher", "com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.fetch.SdaMysqlBinlogInOrderDirectFetcher") //强制sda
    taskRequestBean.getMysql2MysqlRunningInfoBean.setBatcherNameToLoad(new util.HashMap[String, String]())
    taskRequestBean.getMysql2MysqlRunningInfoBean.getBatcherNameToLoad.put("specialInfoSender", "com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.imp.MysqlBinlogInOrderMysqlSpecialInfoSender4Sda") //强制sda
    taskRequestBean.setSdaBean(new SdaRequestBean(getMappingRule, allEncryptField)) //增加rule
  }

  /**
    * 获取全部表的映射关系
    *
    */
  private def getAllTableMappingByDatabase(concernedDatabases: Set[String]): Map[String, String] = {
    val map = new HttpHeaders
    map.add("token", mataDataToken)
    map.add("accept", "*/*")
    val httpEntity: HttpEntity[Any] = new HttpEntity[Any](null, map)
    restTemplate
      .exchange(mataDataUrl, HttpMethod.GET, httpEntity, classOf[java.util.List[util.LinkedHashMap[String, String]]])
      .getBody
      .asScala
      .withFilter(x => concernedDatabases.contains(x.get("sourceDb").toString.trim.toLowerCase) && x.get("newTable").toString.endsWith(x.get("sourceTable").toString)) //过滤
      .map { x => (s"${x.get("sourceDb").toString.trim.toLowerCase}.${x.get("sourceTable").toString.toLowerCase}" -> s"${x.get("newDb").toString}.${x.get("newTable").toString}") } //全部转为小写

      .toMap
  }

  private def getAllEncryptField(concernedDatabases: List[String]): util.Map[String, java.util.Set[String]] = {
    val sql = s"select db_name,table_name,columns from $encryptFieldInfoTableName where db_name in (${concernedDatabases.map(x => s"'$x'").mkString(",")})"
    jdbcTemplate
      .queryForList(sql)
      .asScala
      .map {
        map =>
          (s"${map.get("db_name")}.${map.get("table_name")}" -> map.get("columns").toString.split(",").toSet.asJava)
      }
      .toMap.asJava
  }
}

object Mysql2MysqlService {
  @volatile var ref: Mysql2MysqlService = null
}
