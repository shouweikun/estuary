package com.neighborhood.aka.laplace.estuary.bean.exception.control

/**
  * Created by john_liu on 2018/6/11.
  */
class PartBuildFailureException(
                                 message: => String,
                                 cause: Throwable
                               ) extends SyncControlException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}
