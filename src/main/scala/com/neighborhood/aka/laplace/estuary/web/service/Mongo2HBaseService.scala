package com.neighborhood.aka.laplace.estuary.web.service

import com.neighborhood.aka.laplace.estuary.web.bean.{Mongo2HBaseTaskRequestBean, Mongo2KafkaTaskRequestBean}
import org.springframework.stereotype.Service

/**
  * Created by john_liu on 2019/3/15.
  */

@Service("mongo2hbase")
final class Mongo2HBaseService extends SyncService[Mongo2HBaseTaskRequestBean]{
  /**
    * 开始一个同步任务
    *
    * @param taskRequestBean 同步任务开始标志
    * @return 任务启动信息
    */
  override protected def startNewOneTask(taskRequestBean: Mongo2HBaseTaskRequestBean): String = ???
}
