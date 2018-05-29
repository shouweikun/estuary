package com.neighborhood.aka.laplace.estuary.bean.exception.fetch

/**
  * Created by john_liu on 2018/5/28.
  */
class OutOfFetchRetryThersholdException(
                                         message: => String,
                                         cause: Throwable
                                       ) extends FetchDataException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}
