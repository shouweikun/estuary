package com.neighborhood.aka.laplace.estuary.mongo.task.kafka

import java.util.Date

import com.neighborhood.aka.laplace.estuary.bean.identity.BaseExtractBean
import com.neighborhood.aka.laplace.estuary.mongo.sink.OplogKeyKafkaBeanImp
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoSourceBeanImp

/**
  * Created by john_liu on 2019/2/28.
  */
final case class Mongo2KafkaAllTaskInfoBean (
                                              sinkBean: OplogKeyKafkaBeanImp,
                                              sourceBean: MongoSourceBeanImp,
                                              taskRunningInfoBean: Mongo2KafkaTaskInfoBeanImp
)extends BaseExtractBean {


  /**
    * 同步任务的唯一id, 这个id表示同步任务的唯一标识
    */
  override val syncTaskId: String = taskRunningInfoBean.syncTaskId

  /**
    * 数据同步形式
    */
  override def dataSyncType: String = taskRunningInfoBean.dataSyncType

  override protected val createTime: Date = new Date()

  override protected val lastChange: Date = createTime
}
