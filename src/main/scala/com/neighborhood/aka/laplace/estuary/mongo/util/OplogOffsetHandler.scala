package com.neighborhood.aka.laplace.estuary.mongo.util

import java.util.concurrent.atomic.AtomicBoolean

import com.alibaba.fastjson.JSON
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import com.neighborhood.aka.laplace.estuary.core.task.PositionHandler
import com.neighborhood.aka.laplace.estuary.core.util.zookeeper.EstuaryStringZookeeperManager
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by john_liu on 2019/3/1.
  *
  * mongo的offset记录器
  *
  * @note 非线程安全
  * @author neighborhood.aka.laplace
  */
final class OplogOffsetHandler(
                                private val zkManager: EstuaryStringZookeeperManager,
                                private val syncTaskId: String,
                                private val inputMongoOffset: Option[MongoOffset] = None
                              ) extends PositionHandler[MongoOffset] {

  override protected lazy val logger: Logger = LoggerFactory.getLogger(classOf[OplogOffsetHandler])
  private val connectionStatus = new AtomicBoolean() //链接状态

  /**
    * 生命周期:启动
    */
  override def start(): Unit = {
    zkManager.start()
    connectionStatus.set(true)
  }

  /**
    * 判断是否启动
    *
    * @return
    */
  override def isStart: Boolean = connectionStatus.get()

  /**
    * 声明周期：关闭
    */
  override def close(): Unit = {
    zkManager.stop()
    connectionStatus.set(false)
  }

  /**
    * 保存任务offset
    *
    * @param destination 同步任务id
    * @param logPosition offset
    */
  override def persistLogPosition(destination: String, logPosition: MongoOffset): Unit = {
    logger.info(s"start to persist log position:${logPosition.formatString},id:$syncTaskId")
    val value =
      s"""
         {
         "mongoTsSecond":${logPosition.mongoTsSecond},
         "mongoTsInc":${logPosition.mongoTsInc}
         }
       """.stripMargin
    zkManager.persistStringBy(destination, value)
    logger.info(s"persist log position finished ,id:$syncTaskId")
  }

  /**
    * 根据任务id获取offset
    *
    * @param destination syncTaskId
    * @note null 可能性
    * @return  offset
    */
  override def getlatestIndexBy(destination: String): MongoOffset = {
    Option(zkManager.getStringBy(destination))
      .map {
        str =>
          val js = JSON.parseObject(str)
          val second = js.get("mongoTsSecond").toString.toInt
          val inc = js.get("mongoTsInc").toString.toInt
          MongoOffset(second, inc)
      }.getOrElse(null)

  }

  /**
    * 事实上没用上conn
    * 1. 从zk中获取
    * 2. 从传入的获取
    * 3. 使用当前时间
    *
    * @param conn
    * @return
    */
  override def findStartPosition(conn: DataSourceConnection): MongoOffset = {
    Option(getlatestIndexBy(syncTaskId))
      .orElse(inputMongoOffset)
      .getOrElse(MongoOffset.now)
  }
}
