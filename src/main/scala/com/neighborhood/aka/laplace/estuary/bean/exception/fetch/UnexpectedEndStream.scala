package com.neighborhood.aka.laplace.estuary.bean.exception.fetch

import com.neighborhood.aka.laplace.estuary.bean.exception.EstuaryException

/**
  * Created by john_liu on 2018/7/4.
  */
class UnexpectedEndStream (
                            message: => String,
                            cause: Throwable
                          ) extends EstuaryException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}
