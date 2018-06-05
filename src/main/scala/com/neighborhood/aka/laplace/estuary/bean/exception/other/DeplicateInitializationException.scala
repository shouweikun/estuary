package com.neighborhood.aka.laplace.estuary.bean.exception.other

import com.neighborhood.aka.laplace.estuary.bean.exception.EstuaryException

/**
  * Created by john_liu on 2018/6/4.
  */
class DeplicateInitializationException (
                                         message: => String,
                                         cause: Throwable
                                       ) extends EstuaryException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}
