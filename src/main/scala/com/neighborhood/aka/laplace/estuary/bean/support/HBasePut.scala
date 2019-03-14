package com.neighborhood.aka.laplace.estuary.bean.support

import com.neighborhood.aka.laplace.estuary.core.offset.ComparableOffset
import org.apache.hadoop.hbase.client.Put

/**
  * Created by john_liu on 2019/3/14.
  *
  * @author neighborhood.aka.laplace
  */
final case class HBasePut[A <: ComparableOffset[A]](
                                                     val key: String,
                                                     val put: Put,
                                                     val offset: A
                                                   ) {

}
