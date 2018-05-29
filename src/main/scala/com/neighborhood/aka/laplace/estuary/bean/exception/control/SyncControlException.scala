package com.neighborhood.aka.laplace.estuary.bean.exception.control

import com.neighborhood.aka.laplace.estuary.bean.exception.EstuaryException

/**
  * Created by john_liu on 2018/5/28.
  */
class SyncControlException(
                            message: => String,
                            cause: Throwable
                          ) extends EstuaryException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}