package com.neighborhood.aka.laplace.estuary.web.service

import com.neighborhood.aka.laplace.estuary.core.akkaUtil.SyncDaemonCommand.ExternalStartCommand
import com.neighborhood.aka.laplace.estuary.core.task.Mongo2KafkaSyncTask
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.control.Oplog2KafkaController
import com.neighborhood.aka.laplace.estuary.web.akkaUtil.ActorRefHolder
import com.neighborhood.aka.laplace.estuary.web.bean.Mongo2KafkaTaskRequestBean
import com.neighborhood.aka.laplace.estuary.web.utils.TaskBeanTransformUtil
import org.springframework.stereotype.Service

/**
  * Created by john_liu on 2019/3/7.
  */
@Service("mongo2kafka")
class Mongo2KafkaService extends SyncService[Mongo2KafkaTaskRequestBean] {
  /**
    * 开始一个同步任务
    *
    * @param taskRequestBean 同步任务开始标志
    * @return 任务启动信息
    */
  override protected def startNewOneTask(taskRequestBean: Mongo2KafkaTaskRequestBean): String = {
    val taskInfo = TaskBeanTransformUtil.convertMongo2KafkaRequest2Mongo2KafkaTaskInfo(taskRequestBean)
    ActorRefHolder.syncDaemon ! ExternalStartCommand(Mongo2KafkaSyncTask(Oplog2KafkaController.props(taskInfo), taskRequestBean.getMongo2KafkaRunningInfoRequestBean.getSyncTaskId))
    s"""
      {
       "syncTaskId":"${taskRequestBean.getMongo2KafkaRunningInfoRequestBean.getSyncTaskId}",
       "status":"submitted"
      }
    """.stripMargin
  }
}
