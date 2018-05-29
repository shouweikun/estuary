package com.neighborhood.aka.laplace.estuary.bean.exception.control

/**
  * Created by john_liu on 2018/5/28.
  */
class WorkerCannotFindException(
                                 message: => String,
                                 cause: Throwable
                               ) extends SyncControlException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}