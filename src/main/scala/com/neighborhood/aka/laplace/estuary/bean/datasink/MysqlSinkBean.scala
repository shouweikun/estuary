package com.neighborhood.aka.laplace.estuary.bean.datasink

import com.neighborhood.aka.laplace.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc

/**
  * Created by john_liu on 2019/1/13.
  */
trait MysqlSinkBean extends DataSinkBean[MysqlSinkFunc] {

  def dataSinkType: String = SinkDataType.MYSQL.toString

  /**
    * 数据汇Mysql的链接信息
    */
  def credential: MysqlCredentialBean
}
