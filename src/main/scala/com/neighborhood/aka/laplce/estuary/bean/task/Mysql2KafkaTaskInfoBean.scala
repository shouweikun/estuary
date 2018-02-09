package com.neighborhood.aka.laplce.estuary.bean.task

import java.nio.charset.Charset

import com.neighborhood.aka.laplce.estuary.bean.datasink.KafkaBean
import com.neighborhood.aka.laplce.estuary.bean.identity.BaseExtractBean
import com.neighborhood.aka.laplce.estuary.bean.identity.DataSyncType.DataSyncType
import com.neighborhood.aka.laplce.estuary.bean.resource.MysqlBean

/**
  * Created by john_liu on 2018/2/7.
  */
class Mysql2KafkaTaskInfoBean extends MysqlBean with KafkaBean with BaseExtractBean{


  /**
    * 数据同步形式
    */
  override var dataSyncType: DataSyncType = _
  //从库id
  var slaveId :Long = System.currentTimeMillis()
  //binlog
  /**
    * binlog文件名称
    */
   var journalName:String = null
  /**
    * 在binlog中的偏移量信息
    */
   var position = 0L
  /**
    * binlog中的时间标记
    */
   var timestamp = 0L

  /**
    * 是否计数，默认不计数
    */
  var isCounting :Boolean = false
  /**
    * 是否计算每条数据的时间，默认不计时
    */
  var isProfiling :Boolean = false
  /**
    * 是否事务写
    * 默认是
    * 如果否的话，就是并行写
    */
  var isTransactional:Boolean = true


}
