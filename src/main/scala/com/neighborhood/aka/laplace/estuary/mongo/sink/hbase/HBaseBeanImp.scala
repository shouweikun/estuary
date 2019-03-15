package com.neighborhood.aka.laplace.estuary.mongo.sink.hbase

import com.neighborhood.aka.laplace.estuary.bean.datasink.HBaseBean

/**
  * Created by john_liu on 2019/3/15.
  */

final case class HBaseBeanImp(
                    override val HbaseZookeeperQuorum: String,
                    override val HabseZookeeperPropertyClientPort: String
                  ) extends HBaseBean {

}
