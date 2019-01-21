package com.neighborhood.aka.laplace.estuary.bean.exception.snapshot

/**
  * Created by john_liu on 2018/7/13.
  */
class UnmatchedTaskInfoException (
                                   message: => String,
                                   cause: Throwable
                                 ) extends SnapshotException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}
