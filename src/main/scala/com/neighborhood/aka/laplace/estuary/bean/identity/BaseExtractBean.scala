package com.neighborhood.aka.laplace.estuary.bean.identity

/**
  * Created by john_liu on 2018/2/7.
  */
trait BaseExtractBean extends BaseBean {

  /**
    * 描述
    */
  def describe: String = ""
  /**
    * 数据同步形式
    */
  //  var dataSyncType : DataSyncType
  def dataSyncType: String
}
