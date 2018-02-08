package com.neighborhood.aka.laplce.estuary.core.task

import com.neighborhood.aka.laplce.estuary.core.lifecycle.Status
import com.neighborhood.aka.laplce.estuary.core.lifecycle.Status.Status

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
  /**
    * 任务运行状态
    * 此trait的实现类可以扩展此方法返回具体部件的状态
    */
  def taskStatus:Map[String,Status] = {
    val thisTaskStatus = status
    Map("task"->thisTaskStatus)
  }

}
object TaskManager {

}
