package com.neighborhood.aka.laplace.estuary.bean.exception.snapshot

import com.neighborhood.aka.laplace.estuary.bean.exception.EstuaryException

/**
  * Created by john_liu on 2018/7/9.
  */
abstract class SnapshotException (
                          message: => String,
                          cause: Throwable
                        ) extends EstuaryException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}
