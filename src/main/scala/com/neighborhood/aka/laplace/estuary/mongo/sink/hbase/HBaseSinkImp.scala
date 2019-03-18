package com.neighborhood.aka.laplace.estuary.mongo.sink.hbase

import com.neighborhood.aka.laplace.estuary.bean.datasink.HBaseBean
import com.neighborhood.aka.laplace.estuary.core.sink.hbase.HBaseSinkFunc

/**
  * Created by john_liu on 2019/3/15.
  */
final class HBaseSinkImp(override val hbaseSinkBean: HBaseBean) extends HBaseSinkFunc(hbaseSinkBean) {

}
