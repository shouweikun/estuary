package com.neighborhood.aka.laplace.estuary.bean.identity

import java.util.Date

/**
  * Created by john_liu on 2018/2/7.
  * 标识唯一任务
  */
trait BaseBean {
  /**
    * 同步任务的唯一id, 这个id表示同步任务的唯一标识
    */
  def syncTaskId: String

  protected def createTime: Date

  protected def lastChange: Date

  protected def version = 0L

}
