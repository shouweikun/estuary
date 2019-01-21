package com.neighborhood.aka.laplace.estuary.bean.exception.schema

/**
  * Created by john_liu on 2018/8/29.
  */
class SourceSchemaNoFoundException(
                                    message: => String,
                                    cause: Throwable
                                  ) extends SchemaException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}

