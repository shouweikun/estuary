package com.neighborhood.aka.laplace.estuary.web.service

import com.neighborhood.aka.laplace.estuary.core.akkaUtil.SyncDaemonCommand.ExternalStartCommand
import com.neighborhood.aka.laplace.estuary.core.task.{Mongo2HBaseSyncTask, Mongo2KafkaSyncTask}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.control.hbase.{Oplog2HBaseController, Oplog2HBaseMutliInstanceController}
import com.neighborhood.aka.laplace.estuary.mongo.lifecycle.control.kafka.Oplog2KafkaController
import com.neighborhood.aka.laplace.estuary.web.akkaUtil.ActorRefHolder
import com.neighborhood.aka.laplace.estuary.web.bean.Mongo2HBaseTaskRequestBean
import com.neighborhood.aka.laplace.estuary.web.utils.TaskBeanTransformUtil
import org.springframework.stereotype.Service

/**
  * Created by john_liu on 2019/3/15.
  */

@Service("mongo2hbase")
final class Mongo2HBaseService extends SyncService[Mongo2HBaseTaskRequestBean] {
  /**
    * 开始一个同步任务
    *
    * @param taskRequestBean 同步任务开始标志
    * @return 任务启动信息
    */
  override protected def startNewOneTask(taskRequestBean: Mongo2HBaseTaskRequestBean): String = {
    val taskInfoBean = TaskBeanTransformUtil.convertMongo2HBaseRequest2Mongo2HBaseTaskInfo(taskRequestBean)
    val props = if (taskRequestBean.isMutli) Oplog2HBaseMutliInstanceController.props(taskInfoBean) else Oplog2HBaseController.props(taskInfoBean)

    ActorRefHolder.syncDaemon ! ExternalStartCommand(Mongo2HBaseSyncTask(props, taskRequestBean.getMongo2HBaseRunningInfo.getSyncTaskId))
    s"""
      {
       "syncTaskId":"${taskRequestBean.getMongo2HBaseRunningInfo.getSyncTaskId}",
       "status":"submitted"
      }
    """.stripMargin
  }
}
