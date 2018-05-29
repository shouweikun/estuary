package com.neighborhood.aka.laplace.estuary.bean.exception

/**
  * Created by john_liu on 2018/5/28.
  */
abstract class EstuaryException(
                                 message: => String,
                                 cause: Throwable
                               )
  extends Exception(message, cause) {


  def this(message: => String) = this(message, null)

  def this(cause: Throwable) = this("", cause)

  var binlogJournalName = ""
  var binlogOffset = 4l
}

