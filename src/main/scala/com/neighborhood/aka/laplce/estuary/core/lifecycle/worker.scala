package com.neighborhood.aka.laplce.estuary.core.lifecycle

import com.neighborhood.aka.laplce.estuary.core.lifecycle.Status.Status

/**
  * Created by john_liu on 2018/2/8.
  */
trait worker {
  /**
    * 错位次数阈值
    */
  var errorCountThreshold: Int
  /**
    * 错位次数
    */
  var errorCount: Int

  /**
    * 错误次数超过重试次数时，返回true
    */
  def isCrashed: Boolean = errorCount >= errorCountThreshold

  /**
    * 错误处理
    */
  def processError(e: Throwable, message: WorkerMessage)
  /**
    * 状态变化
    */
  def changeStatus(status: Status,changFunc: Status => Unit, onChangeFunc: => Unit):Unit = {
    changFunc(status)
    onChangeFunc
  }

}
