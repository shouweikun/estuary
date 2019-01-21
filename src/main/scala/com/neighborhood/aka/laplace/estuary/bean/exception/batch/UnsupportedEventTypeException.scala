package com.neighborhood.aka.laplace.estuary.bean.exception.batch

/**
  * Created by john_liu on 2018/6/14.
  */
class UnsupportedEventTypeException (
                                      message: => String,
                                      cause: Throwable
                                    ) extends BatchException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)

}