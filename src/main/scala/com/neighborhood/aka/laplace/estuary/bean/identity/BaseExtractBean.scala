package com.neighborhood.aka.laplace.estuary.bean.identity

import DataSyncType.DataSyncType
import org.mongodb.morphia.annotations.Indexed

/**
  * Created by john_liu on 2018/2/7.
  */
trait BaseExtractBean extends BaseBean{
  /**
    * 同步任务的唯一id, 这个id表示同步任务的唯一标识
    */
  var syncTaskId :String= _
  /**
    * 描述
    */
  var describe :String = ""
  /**
    * 数据同步形式
    */
  var dataSyncType : DataSyncType
}
