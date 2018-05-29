package com.neighborhood.aka.laplace.estuary.bean.exception.sink

/**
  * Created by john_liu on 2018/5/28.
  */
class KafkaSinkSendFailureException(
                                     message: => String,
                                     cause: Throwable
                                   ) extends SinkDataException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}

