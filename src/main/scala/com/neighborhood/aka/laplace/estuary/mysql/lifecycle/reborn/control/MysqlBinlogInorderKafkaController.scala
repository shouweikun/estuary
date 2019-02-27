package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.control

import com.neighborhood.aka.laplace.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.{SinkManager, SourceManager, TaskManager}
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlConnection
import com.neighborhood.aka.laplace.estuary.mysql.task.kafka.Mysql2KafkaAllTaskInfoBean

/**
  * Created by john_liu on 2019/2/27.
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlBinlogInorderKafkaController(
                                               val totalTaskInfo: Mysql2KafkaAllTaskInfoBean
                                             ) extends MysqlBinlogInOrderController[KafkaSinkFunc[BinlogKey, String]](totalTaskInfo.taskRunningInfoBean, totalTaskInfo.sourceBean, totalTaskInfo.sinkBean) {
  /**
    * 1.初始化HeartBeatsListener
    * 2.初始化binlogSinker
    * 3.初始化binlogEventBatcher
    * 4.初始化binlogFetcher
    * 5.初始化processingCounter
    * 6.初始化powerAdapter
    * 7.初始化positionRecorder
    */
  override def initWorkers: Unit = {

  }

  /**
    * 任务资源管理器的构造的工厂方法
    *
    * @return 构造好的资源管理器
    *
    */
  override def buildManager: SourceManager[MysqlConnection] with SinkManager[KafkaSinkFunc[BinlogKey, String]] with TaskManager = {
    ???
  }
}
