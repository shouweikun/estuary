package com.neighborhood.aka.laplce.estuary.core.lifecycle

/**
  * Created by john_liu on 2018/2/8.
  */
trait worker {

  var errorCountThreshold : Int
  var errorCount :Int
  /**
    * 错误次数超过重试次数时，返回true
    */
  def isCrashed :Boolean = errorCount >= errorCountThreshold
  /**
    * 错误处理
    */
  def processError(e:Throwable,message:WorkerMessage)
}
