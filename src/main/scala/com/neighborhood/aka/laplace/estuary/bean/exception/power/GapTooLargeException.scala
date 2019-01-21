package com.neighborhood.aka.laplace.estuary.bean.exception.power

/**
  * Created by john_liu on 2018/7/26.
  */
class GapTooLargeException(
                            message: => String,
                            cause: Throwable
                          ) extends PowerControlException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}