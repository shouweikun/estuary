package com.neighborhood.aka.laplace.estuary.bean.datasink

import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc

/**
  * Created by john_liu on 2018/2/7.
  */
trait DataSinkBean[S <: SinkFunc] {
  //  var dataSinkType:DataSinkType
  def dataSinkType: String
}
