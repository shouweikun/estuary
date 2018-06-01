package com.neighborhood.aka.laplace.estuary.bean.datasink

/**
  * Created by john_liu on 2018/6/1.
  */
trait HBaseBean extends DataSinkBean {

  val HbaseZookeeperQuorum: String
  val HabseZookeeperPropertyClientPort: String
}
