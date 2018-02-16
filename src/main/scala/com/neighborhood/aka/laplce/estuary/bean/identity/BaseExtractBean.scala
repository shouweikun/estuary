package com.neighborhood.aka.laplce.estuary.bean.identity

import com.neighborhood.aka.laplce.estuary.bean.identity.DataSyncType.DataSyncType
import org.mongodb.morphia.annotations.Indexed

/**
  * Created by john_liu on 2018/2/7.
  */
trait BaseExtractBean extends BaseBean{
  /**
    * 同步任务的唯一id, 这个id表示同步任务的唯一标识
    */
  @Indexed(unique = true, sparse = false)
  var syncTaskId :String= System.currentTimeMillis.toString
  /**
    * 描述
    */
  var describe :String = ""
  /**
    * 数据同步形式
    */
  var dataSyncType : DataSyncType
}
