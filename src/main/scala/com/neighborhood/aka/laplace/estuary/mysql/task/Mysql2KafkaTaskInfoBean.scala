package com.neighborhood.aka.laplace.estuary.mysql.task

import java.util.concurrent.atomic.AtomicLong

import com.neighborhood.aka.laplace.estuary.bean.datasink.KafkaBean
import com.neighborhood.aka.laplace.estuary.bean.identity.{BaseExtractBean, SyncDataType}
import com.neighborhood.aka.laplace.estuary.bean.resource.MysqlBean

/**
  * Created by john_liu on 2018/2/7.
  */
final class Mysql2KafkaTaskInfoBean extends MysqlBean with KafkaBean with BaseExtractBean {


  /**
    * 数据同步形式
    */
  //  override var dataSyncType: DataSyncType = DataSyncType.NORMAL
  override var dataSyncType: String = SyncDataType.NORMAL.toString
  //从库id
  var slaveId: Long = System.currentTimeMillis()
  //binlog
  /**
    * binlog文件名称
    */
  var journalName: String = _
  /**
    * 在binlog中的偏移量信息
    */
  var position = 0L
  /**
    * binlog中的时间标记
    */
  var timestamp = 0L

  /**
    * 是否计数，默认不计数
    */
  var isCounting: Boolean = false
  /**
    * 是否计算每条数据的时间，默认不计时
    */
  var isCosting: Boolean = false
  /**
    * 是否保留最新binlog位置
    */
  var isProfiling: Boolean = false
  /**
    * 是否事务写
    * 默认否
    * 如果否的话，就是并行写
    */
  var isTransactional: Boolean = false
  /**
    * 是否打开功率调节器
    */
  var isPowerAdapted: Boolean = false
  /**
    * entry打包的阈值
    */
  var batchThreshold: AtomicLong = new AtomicLong(50)
  /**
    * binlog文件名称
    */
  var batcherNum: Int = 23
  /**
    * 数据拉取时延
    * 单位微秒
    */
  val fetchDelay: AtomicLong = new AtomicLong(2000000)
  /**
    * 同步关心的表
    */
  var concernedDatabase = ""
  /**
    * 同步忽略的表
    */
  var ignoredDatabase = ""
  /**
    * 监听心跳
    */
  var listenTimeout = 5000
  var listenRetrytime = 3
  // 支持的binlogImage
  var binlogImages = ""
  //支持的binlogFormat
  var binlogFormat = ""
  //zookeeper地址,可以设置多个，用";"分隔
  var zookeeperServers = "10.10.248.207:2181;10.10.237.78:2181"
  // zookeeper 链接超时设置,单位毫秒
  var zookeeperTimeout = 10000
}
object Mysql2KafkaTaskInfoBean {
  def apply: Mysql2KafkaTaskInfoBean = new Mysql2KafkaTaskInfoBean()
}