package com.neighborhood.aka.laplace.estuary.bean.datasink

import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc

/**
  * Created by john_liu on 2018/6/1.
  *
  * @todo 未完成
  */

trait HBaseBean extends DataSinkBean[MysqlSinkFunc] {

  def HbaseZookeeperQuorum: String
  def HabseZookeeperPropertyClientPort: String
  override val dataSinkType: String  =SinkDataType.HBASE.toString
}
