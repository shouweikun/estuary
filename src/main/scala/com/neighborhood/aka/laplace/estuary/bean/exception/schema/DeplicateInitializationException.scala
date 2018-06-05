package com.neighborhood.aka.laplace.estuary.bean.exception.schema

import com.neighborhood.aka.laplace.estuary.bean.exception.EstuaryException

/**
  * Created by john_liu on 2018/6/4.
  */
class DeplicateInitializationException(
                                        message: => String,
                                        cause: Throwable
                                      ) extends SchemaException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}
