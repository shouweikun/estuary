package com.neighborhood.aka.laplace.estuary.core.trans

/**
  * Created by john_liu on 2018/5/1.
  */
trait MappingFormat[A, B] {

  /**
    * 拼接json用
    */
  val START_JSON = "{"
  val END_JSON = "}"
  val START_ARRAY = "["
  val END_ARRAY = "]"
  val KEY_VALUE_SPLIT = ":"
  val ELEMENT_SPLIT = ","
  val STRING_CONTAINER = "\""

  def transform(x:A):Iterable[B]
}
