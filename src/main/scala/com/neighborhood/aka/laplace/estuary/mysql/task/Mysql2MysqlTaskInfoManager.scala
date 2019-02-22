package com.neighborhood.aka.laplace.estuary.mysql.task

import akka.actor.ActorRef
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.mappings.{CanalEntry2RowDataInfoMappingFormat4Sda, DefaultCanalEntry2RowDataInfoMappingFormat}
import com.neighborhood.aka.laplace.estuary.mysql.schema.SdaSchemaMappingRule
import com.neighborhood.aka.laplace.estuary.mysql.schema.tablemeta.{EstuaryMysqlColumnInfo, EstuaryMysqlTableMeta, MysqlTableSchemaHolder}
import com.neighborhood.aka.laplace.estuary.mysql.sink.{MysqlSinkBeanImp, MysqlSinkManagerImp}
import com.neighborhood.aka.laplace.estuary.mysql.source.{MysqlSourceBeanImp, MysqlSourceManagerImp}
import com.typesafe.config.Config

import scala.util.Try

/**
  * Created by john_liu on 2019/1/15.
  * Mysql TO Mysql 的任务信息与资源管理器
  *
  * @author neighborhood.aka.laplace
  */
final class Mysql2MysqlTaskInfoManager(
                                        taskInfoBean: Mysql2MysqlTaskInfoBean,
                                        _config: Config
                                      ) extends MysqlSinkManagerImp with MysqlSourceManagerImp with TaskManager {
  /**
    * 传入的配置
    *
    */
  override val config: Config = _config

  /**
    * 任务信息bean
    */
  override val taskInfo: Mysql2MysqlTaskInfoBean = taskInfoBean
  /**
    * 数据汇bean
    */
  override val sinkBean: MysqlSinkBeanImp = taskInfo.sinkBean

  /**
    * mysql任务信息
    */
  override val sourceBean: MysqlSourceBeanImp = taskInfo.sourceBean

  /**
    * 保存offset的zk地址
    */
  override val offsetSaveZkServers: String = taskInfo.taskRunningInfoBean.offsetZkServers

  /**
    * 开始的任务位点
    */
  override lazy val startPosition: Option[EntryPosition] = taskInfo.taskRunningInfoBean.startPosition


  /**
    * batch转换模块
    */
  override lazy val batchMappingFormat: Option[MappingFormat[_, _]] = Option(buildMappingFormat)

  /**
    * 事件溯源的事件收集器
    */
  override lazy val eventCollector: Option[ActorRef] = None //todo 生成事件收集器

  /**
    * 是否启动Schema管理模块,保存元数据
    */
  override val schemaComponentIsOn: Boolean = taskInfo.taskRunningInfoBean.schemaComponentIsOn
  /**
    * 是否开启数据Schema校验对比
    * 如果Schema管理模块没有开启，不支持就不支持该功能
    */
  val isCheckSinkSchema: Boolean = schemaComponentIsOn && taskInfo.taskRunningInfoBean.isCheckSinkSchema
  /**
    * 是否需要执行ddl
    */
  override val isNeedExecuteDDL: Boolean = taskInfo.taskRunningInfoBean.isNeedExecuteDDL

  /**
    * 是否计数，默认不计数
    */
  override val isCounting: Boolean = taskInfo.taskRunningInfoBean.isCounting

  /**
    * 是否计算每条数据的时间，默认不计时
    */
  override val isCosting: Boolean = taskInfo.taskRunningInfoBean.isCosting

  /**
    * 是否保留最新binlog位置
    */
  override val isProfiling: Boolean = taskInfo.taskRunningInfoBean.isProfiling

  /**
    * 是否打开功率调节器
    */
  override val isPowerAdapted: Boolean = taskInfo.taskRunningInfoBean.isPowerAdapted

  /**
    * 是否同步写
    */
  override val isSync: Boolean = true //todo

  /**
    * 是否是补录任务
    */
  override val isDataRemedy: Boolean = false //todo 暂时还不支持

  /**
    * 任务类型
    * 由三部分组成
    * DataSourceType-DataSyncType-DataSinkType
    */
  override val taskType: String = s"${sourceBean.dataSourceType}-${taskInfo.dataSyncType}-${sinkBean.dataSinkType}"


  /**
    * 分区模式
    *
    */
  override val partitionStrategy: PartitionStrategy = taskInfo.taskRunningInfoBean.partitionStrategy

  /**
    * 是否阻塞式拉取
    *
    */
  override val isBlockingFetch: Boolean = true //todo 不支持异步写

  /**
    * 同步任务开始时间 用于fetch过滤无用字段
    *
    * @return
    */
  override val syncStartTime: Long = taskInfo.taskRunningInfoBean.syncStartTime

  /**
    * 加载的sinker的名称
    */
  override val sinkerNameToLoad: Map[String, String] = taskInfo.taskRunningInfoBean.sinkerNameToLoad

  /**
    * 加载的fetcherName
    */
  override val fetcherNameToLoad: Map[String, String] = taskInfo.taskRunningInfoBean.fetcherNameToLoad

  /**
    * 加载的controller的名称
    */
  override val controllerNameToLoad: Map[String, String] = taskInfo.taskRunningInfoBean.controllerNameToLoad

  /**
    * 加载的batcher的名称
    */
  override val batcherNameToLoad: Map[String, String]
  = taskInfo.taskRunningInfoBean.batcherNameToLoad

  /**
    * 同步任务标识
    */
  override val syncTaskId: String = taskInfo.taskRunningInfoBean.syncTaskId

  /**
    * 打包阈值
    */
  override val batchThreshold: Long = taskInfo.taskRunningInfoBean.batchThreshold

  /**
    * batcher的数量
    */
  override val batcherNum: Int = taskInfo.taskRunningInfoBean.batcherNum

  /**
    * sinker的数量
    */
  override val sinkerNum: Int = batcherNum


  /**
    * sda专用属性,table对应规则
    */
  lazy val tableMappingRule: SdaSchemaMappingRule = new SdaSchemaMappingRule(taskInfo.sdaBean.map(_.tableMappingRule).getOrElse(Map.empty))


  /**
    * sink端的元数据信息
    */
  lazy val sinkMysqlTableSchemaHolder: MysqlTableSchemaHolder = buildMysqlTableSchemaHolderFromSink


  /**
    * 初始化/启动
    */
  override def start: Unit = {
    tableMappingRule
    sinkMysqlTableSchemaHolder
  }

  /**
    * 关闭
    * 当与资源管理器eg:SinkManager和SourceManager绑定时，将资源关闭交给这个方法
    */
  override def close: Unit = Try {
    closeSource
    closeSink
  }

  /**
    * 从Sink端的Information Schema构造mysqlTableSchemaHolder
    *
    * @return
    */
  def buildMysqlTableSchemaHolderFromSink: MysqlTableSchemaHolder = {
    //这么做的理由是获取sink端的databaseName,防止由于source 和sink tableName对应不上的问题
    val dbs = concernedDatabase.map(x => Option(tableMappingRule).flatMap(_.getDatabaseMappingName(x)).getOrElse(x)) //如果匹配不到sda的，就使用原来的
    val map = MysqlTableSchemaHolder.getTableSchemasByDbName(dbs, sink).map {
      x =>
        val columnName = x("COLUMN_NAME".toLowerCase).toString
        val mysqlType = x("DATA_TYPE".toLowerCase).toString
        val index = x("ORDINAL_POSITION".toLowerCase).toString.toInt - 1
        s"${x("TABLE_SCHEMA".toLowerCase)}.${x("TABLE_NAME".toLowerCase)}" -> EstuaryMysqlColumnInfo(columnName, index, mysqlType)
    }.groupBy(x => x._1) //聚类
      .map {
      case (fullName, columns) => fullName -> EstuaryMysqlTableMeta(fullName.split('.')(0), fullName.split('.')(1), columns.map(_._2))
    }
    val createSqlMap = map.mapValues { tableMeta => tableMeta.copy(createTableSql = MysqlTableSchemaHolder.getCreateTableSql(tableMeta.schemaName, tableMeta.tableName, sink)) }
    new MysqlTableSchemaHolder(createSqlMap)
  }

  /**
    * 构造MappingFormat
    *
    * @return mappingFormat
    */
  def buildMappingFormat: MappingFormat[_, _] = {

    def default = new DefaultCanalEntry2RowDataInfoMappingFormat(partitionStrategy, syncTaskId, syncStartTime, schemaComponentIsOn, config, isCheckSinkSchema, Option(sinkMysqlTableSchemaHolder))

    def sda = new CanalEntry2RowDataInfoMappingFormat4Sda(partitionStrategy, syncTaskId, syncStartTime, schemaComponentIsOn, isCheckSinkSchema, config, Option(sinkMysqlTableSchemaHolder), tableMappingRule, taskInfo.sdaBean.map(_.encryptField).getOrElse(Map.empty))


    taskInfo.taskRunningInfoBean.batchMappingFormatName
      .map {
        name =>
          name match {
            case "sda" => sda
            case "default" => default
            case _ => default
          }
      }.getOrElse(default)

  }

}

object Mysql2MysqlTaskInfoManager {
  def apply(
             taskInfoBean: Mysql2MysqlTaskInfoBean,
             _config: Config
           ): Mysql2MysqlTaskInfoManager = new Mysql2MysqlTaskInfoManager(taskInfoBean, _config)
}