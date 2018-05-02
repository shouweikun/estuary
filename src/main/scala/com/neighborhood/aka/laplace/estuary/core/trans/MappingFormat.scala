package com.neighborhood.aka.laplace.estuary.core.trans

/**
  * Created by john_liu on 2018/5/1.
  */
trait MappingFormat[A, B] {
  def transform(x:A):B
}
