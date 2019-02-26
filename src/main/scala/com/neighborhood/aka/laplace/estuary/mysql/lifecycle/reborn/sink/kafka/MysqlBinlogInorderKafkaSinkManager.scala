package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink.kafka

import com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype.SourceDataSinkerManagerPrototype
import com.neighborhood.aka.laplace.estuary.core.sink.kafka.KafkaSinkFunc
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkManagerImp

/**
  * Created by john_liu on 2019/2/26.
  */
abstract class MysqlBinlogInorderKafkaSinkManager[K, V](
                                                         val taskManager: MysqlSinkManagerImp with TaskManager
                                                       ) extends SourceDataSinkerManagerPrototype[KafkaSinkFunc[K, V]] {

}
