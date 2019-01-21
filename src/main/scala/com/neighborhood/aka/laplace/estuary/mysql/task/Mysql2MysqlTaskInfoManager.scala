package com.neighborhood.aka.laplace.estuary.mysql.task

import akka.actor.ActorRef
import com.alibaba.otter.canal.protocol.position.EntryPosition
import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.mappings.{CanalEntry2RowDataInfoMappingFormat, CanalEntry2RowDataInfoMappingFormat4Sda}
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
  override def config: Config = _config

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
  override val startPosition: Option[EntryPosition] = taskInfo.taskRunningInfoBean.startPosition

  /**
    * 是否需要执行ddl
    */
  override def isNeedExecuteDDL: Boolean = taskInfo.taskRunningInfoBean.isNeedExecuteDDL

  /**
    * batch转换模块
    */
  override val batchMappingFormat: Option[MappingFormat[_, _]] = Option(buildMappingFormat)

  /**
    * 事件溯源的事件收集器
    */
  override val eventCollector: Option[ActorRef] = None //todo 生成事件收集器


  /**
    * 是否计数，默认不计数
    */
  override def isCounting: Boolean = taskInfo.taskRunningInfoBean.isCounting

  /**
    * 是否计算每条数据的时间，默认不计时
    */
  override def isCosting: Boolean = taskInfo.taskRunningInfoBean.isCosting

  /**
    * 是否保留最新binlog位置
    */
  override def isProfiling: Boolean = taskInfo.taskRunningInfoBean.isProfiling

  /**
    * 是否打开功率调节器
    */
  override def isPowerAdapted: Boolean = taskInfo.taskRunningInfoBean.isPowerAdapted

  /**
    * 是否同步写
    */
  override def isSync: Boolean = true //todo

  /**
    * 是否是补录任务
    */
  override def isDataRemedy: Boolean = false //todo 暂时还不支持


  /**
    * 任务类型
    * 由三部分组成
    * DataSourceType-DataSyncType-DataSinkType
    */
  override def taskType: String = s"${sourceBean.dataSourceType}-${taskInfo.dataSyncType}-${sinkBean.dataSinkType}"

  /**
    * 是否启动Schema管理模块
    */
  override def schemaComponentIsOn: Boolean = taskInfo.taskRunningInfoBean.schemaComponentIsOn

  /**
    * 分区模式
    *
    */
  override def partitionStrategy: PartitionStrategy = taskInfo.taskRunningInfoBean.partitionStrategy

  /**
    * 是否阻塞式拉取
    *
    */
  override def isBlockingFetch: Boolean = true //todo 不支持异步写

  /**
    * 同步任务开始时间 用于fetch过滤无用字段
    *
    * @return
    */
  override def syncStartTime: Long = taskInfo.taskRunningInfoBean.syncStartTime

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
  override def batchThreshold: Long = taskInfo.taskRunningInfoBean.batchThreshold

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
  val tableMappingRule: Map[String, String] = taskInfo.sdaBean.map(_.tableMappingRule).getOrElse(Map.empty)

  /**
    * 关闭
    * 当与资源管理器eg:SinkManager和SourceManager绑定时，将资源关闭交给这个方法
    */
  override def close: Unit = Try {
    closeSource
    closeSink

  }

  def buildMappingFormat: MappingFormat[_, _] = {

    lazy val default = new CanalEntry2RowDataInfoMappingFormat(partitionStrategy, syncTaskId, syncStartTime, mysqlSchemaHandler, schemaComponentIsOn, config)
    lazy val sda = new CanalEntry2RowDataInfoMappingFormat4Sda(partitionStrategy, syncTaskId, syncStartTime, mysqlSchemaHandler, schemaComponentIsOn, config, tableMappingRule)
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