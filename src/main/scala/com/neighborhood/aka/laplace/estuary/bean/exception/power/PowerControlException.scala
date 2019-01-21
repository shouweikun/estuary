package com.neighborhood.aka.laplace.estuary.bean.exception.power

import com.neighborhood.aka.laplace.estuary.bean.exception.EstuaryException

/**
  * Created by john_liu on 2018/7/26.
  */
abstract class PowerControlException(
                             message: => String,
                             cause: Throwable
                           ) extends EstuaryException(message, cause) {
  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)
}