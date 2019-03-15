package com.neighborhood.aka.laplace.estuary.bean.support

import com.neighborhood.aka.laplace.estuary.bean.key.BaseDataJsonKey

/**
  * Created by john_liu on 2018/3/8.
  *
  * @author neighborhood.aka.laplace
  */


final case class KafkaMessage(
                               val baseDataJsonKey: BaseDataJsonKey,
                               val jsonValue: String,
                               val isAbnormal: Boolean = false
                             ) {


}

object KafkaMessage {
  private lazy val abnormal_ = KafkaMessage(null, null, true)

  def abnormal(key: BaseDataJsonKey) = KafkaMessage(key, null, true)

  def abnormal =abnormal_
}


