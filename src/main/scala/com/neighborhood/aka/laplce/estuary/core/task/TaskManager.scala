package com.neighborhood.aka.laplce.estuary.core.task

import com.neighborhood.aka.laplce.estuary.core.lifecycle.Status

/**
  * Created by john_liu on 2018/2/7.
  * 负责管理资源和任务
  */
trait TaskManager {
  /**
    * 当前任务运行情况
    */
  @volatile
  var status = Status.OFFLINE
  /**
    * 任务类型
    * 由三部分组成
    * DataSourceType-DataSyncType-DataSinkType
    */
  def taskType:String


}
object TaskManager {

}
