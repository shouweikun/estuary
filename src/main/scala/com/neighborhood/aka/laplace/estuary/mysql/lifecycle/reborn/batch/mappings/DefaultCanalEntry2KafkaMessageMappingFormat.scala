package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.mappings

import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy
import com.typesafe.config.Config

/**
  * Created by john_liu on 2019/2/26.
  *
  * @author neighborhood.aka.laplace
  */
final class DefaultCanalEntry2KafkaMessageMappingFormat(
                                                         /**
                                                           * 分区策略
                                                           */
                                                         override val partitionStrategy: PartitionStrategy,

                                                         /**
                                                           * 同步任务Id
                                                           */
                                                         override val syncTaskId: String,

                                                         /**
                                                           * 任务开始时间
                                                           */
                                                         override val syncStartTime: Long,

                                                         /**
                                                           * 是否开启Schema管理
                                                           */
                                                         override val schemaComponentIsOn: Boolean,

                                                         /**
                                                           * config
                                                           */
                                                         override val config: Config
                                                       ) extends CanalEntry2KafkaMessageMappingFormat {

}
