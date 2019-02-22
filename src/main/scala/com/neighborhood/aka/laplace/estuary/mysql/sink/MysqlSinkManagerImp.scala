package com.neighborhood.aka.laplace.estuary.mysql.sink

import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc
import com.neighborhood.aka.laplace.estuary.core.source.MysqlHikariCpConnection
import com.neighborhood.aka.laplace.estuary.core.task.SinkManager

/**
  * Created by john_liu on 2019/1/15.
  *
  * @author neighborhood.aka.laplace
  */
trait MysqlSinkManagerImp extends SinkManager[MysqlSinkFunc] {
  /**
    * 数据汇bean
    */
  override def sinkBean: MysqlSinkBeanImp

  /**
    * 构建数据汇
    *
    * @return sink
    */
  override def buildSink: MysqlSinkFunc = {
    new MysqlSinkFunc(new MysqlHikariCpConnection(sinkBean.hikariConfig))
  }


}
