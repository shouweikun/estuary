package com.neighborhood.aka.laplce.estuary.core.lifecycle

/**
  * Created by john_liu on 2018/2/8.
  */
trait worker {

  var errorCountThreshold : Int
  var errorCount :Int

  def isCrashed :Boolean = errorCount >= errorCountThreshold
}
