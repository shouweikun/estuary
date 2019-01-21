package com.neighborhood.aka.laplace.estuary.bean.exception.batch

/**
  * Created by john_liu on 2018/5/30.
  */
class StoreValueParseFailureException (
                                        message: => String,
                                        cause: Throwable
                                      ) extends BatchException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)

}
