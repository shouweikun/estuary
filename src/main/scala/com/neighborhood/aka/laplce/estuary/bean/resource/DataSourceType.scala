package com.neighborhood.aka.laplce.estuary.bean.resource

/**
  * Created by john_liu on 2018/2/7.
  */
object DataSourceType extends Enumeration{

  type DataSourceType = Value
  val MYSQL = Value(0)
  val MONGO = Value(1)
}
