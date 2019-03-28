package com.neighborhood.aka.laplace.estuary.bean.exception.fetch

import com.neighborhood.aka.laplace.estuary.bean.exception.EstuaryException

/**
  * Created by john_liu on 2019/3/28.
  */
class FetcherTimeoutException (
                                message: => String,
                                cause: Throwable
                              ) extends EstuaryException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}