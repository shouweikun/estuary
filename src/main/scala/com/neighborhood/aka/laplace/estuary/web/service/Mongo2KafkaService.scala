package com.neighborhood.aka.laplace.estuary.web.service

import com.neighborhood.aka.laplace.estuary.web.bean.{Mongo2KafkaTaskRequestBean, Mysql2MysqlRequestBean}

/**
  * Created by john_liu on 2019/3/7.
  */
class Mongo2KafkaService extends SyncService[Mongo2KafkaTaskRequestBean] {
  /**
    * 开始一个同步任务
    *
    * @param taskRequestBean 同步任务开始标志
    * @return 任务启动信息
    */
  override protected def startNewOneTask(taskRequestBean: Mongo2KafkaTaskRequestBean): String = ???
}
