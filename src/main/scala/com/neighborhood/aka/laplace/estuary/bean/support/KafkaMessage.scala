package com.neighborhood.aka.laplace.estuary.bean.support

import com.neighborhood.aka.laplace.estuary.bean.key.BaseDataJsonKey

/**
  * Created by john_liu on 2018/3/8.
  */
 class KafkaMessage {
  protected var baseDataJsonKey:BaseDataJsonKey = null
  protected var jsonValue:String = null



  def getBaseDataJsonKey: BaseDataJsonKey = baseDataJsonKey


  def this(baseDataJsonKey: BaseDataJsonKey, jsonValue: String) {
    this()
    this.baseDataJsonKey = baseDataJsonKey
    this.jsonValue = jsonValue
  }



  def setBaseDataJsonKey(baseDataJsonKey: BaseDataJsonKey): Unit = {
    this.baseDataJsonKey = baseDataJsonKey
  }


  def getJsonValue: String = jsonValue

  def setJsonValue(jsonValue: String): Unit = {
    this.jsonValue = jsonValue
  }
}
