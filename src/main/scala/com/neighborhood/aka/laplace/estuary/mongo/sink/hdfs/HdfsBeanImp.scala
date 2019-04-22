package com.neighborhood.aka.laplace.estuary.mongo.sink.hdfs

import com.neighborhood.aka.laplace.estuary.bean.datasink.HdfsBean

/**
  * Created by john_liu on 2019/4/12.
  */
case class HdfsBeanImp(override val hdfsBasePath:String,val fileFlushInterval:Long = 5*1000*60) extends HdfsBean {

}
