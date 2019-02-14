package com.neighborhood.aka.laplace.estuary.bean.exception.schema

/**
  * Created by john_liu on 2019/1/30.
  */
final class InvalidDdlException(
                                 message: => String,
                                 cause: Throwable
                               ) extends SchemaException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}