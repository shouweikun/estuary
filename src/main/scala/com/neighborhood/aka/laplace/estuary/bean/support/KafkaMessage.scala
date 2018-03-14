package com.neighborhood.aka.laplace.estuary.bean.support

import com.neighborhood.aka.laplace.estuary.bean.key.BaseDataJsonKey

/**
  * Created by john_liu on 2018/3/8.
  */
 class KafkaMessage {
  protected var baseDataJsonKey:BaseDataJsonKey = null
  protected var jsonValue:String = null
  private var journalName:String = null
  private var offset:Long = 0L


  def getBaseDataJsonKey: BaseDataJsonKey = baseDataJsonKey


  def this(baseDataJsonKey: BaseDataJsonKey, jsonValue: String) {
    this()
    this.baseDataJsonKey = baseDataJsonKey
    this.jsonValue = jsonValue
  }

  def this(baseDataJsonKey: BaseDataJsonKey, jsonValue: String, journalName: String, offset: Long) {
    this()
    this.baseDataJsonKey = baseDataJsonKey
    this.jsonValue = jsonValue
    this.journalName = journalName
    this.offset = offset
  }

  def setBaseDataJsonKey(baseDataJsonKey: BaseDataJsonKey): Unit = {
    this.baseDataJsonKey = baseDataJsonKey
  }

  def getJournalName: String = journalName

  def setJournalName(journalName: String): Unit = {
    this.journalName = journalName
  }

  def getOffset: Long = offset

  def setOffset(offset: Long): Unit = {
    this.offset = offset
  }

  def getJsonValue: String = jsonValue

  def setJsonValue(jsonValue: String): Unit = {
    this.jsonValue = jsonValue
  }
}
