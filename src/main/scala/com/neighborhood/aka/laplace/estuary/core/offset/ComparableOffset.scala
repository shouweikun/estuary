package com.neighborhood.aka.laplace.estuary.core.offset

/**
  * Created by john_liu on 2019/1/10.
  */
trait ComparableOffset[ComparableOffset] {

  def compare(other: ComparableOffset): Boolean

  def compare(other: ComparableOffset, smaller: Boolean): ComparableOffset = {
    if (compare(other) ^ smaller) other else this.asInstanceOf[ComparableOffset]
  }


}
