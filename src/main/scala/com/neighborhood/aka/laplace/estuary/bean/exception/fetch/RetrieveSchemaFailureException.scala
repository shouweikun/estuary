package com.neighborhood.aka.laplace.estuary.bean.exception.fetch

/**
  * Created by john_liu on 2018/6/1.
  */
class RetrieveSchemaFailureException (
                                       message: => String,
                                       cause: Throwable
                                     ) extends FetchDataException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}
