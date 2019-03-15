package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.mappingFormat

import com.neighborhood.aka.laplace.estuary.bean.support.HBasePut
import com.neighborhood.aka.laplace.estuary.mongo.{SettingConstant, lifecycle}
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoOffset}
import com.neighborhood.aka.laplace.estuary.mongo.util.MD5Utils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by john_liu on 2019/3/15.
  */
class OplogHBaseMappingFormat extends OplogMappingFormat[HBasePut[MongoOffset]] {
  override def syncTaskId: String = ???

  /**
    * mongoConnection 用于u事件反查
    *
    * @return
    */
  override def mongoConnection: MongoConnection = ???

  override def transform(x: lifecycle.OplogClassifier): HBasePut[MongoOffset] = {
    val oplog = x.toOplog
    val tableName = getTableName(oplog.getDbName,oplog.getTableName)
    val docOption = getRealDoc(oplog)
    if (docOption.isEmpty){
      HBasePut.abnormal(tableName,MongoOffset(oplog.getTimestamp.getTime,oplog.getTimestamp.getInc))
    }else{
      val doc = docOption.get
      val put = new Put(Bytes.toBytes(MD5Utils.md5(doc.get("_id").toString)))
      val endTime = System.currentTimeMillis()
      val ts = getTs(oplog.getTimestamp.getTime,oplog.getTimestamp.getInc)
      val dbEffectTime = (oplog.getTimestamp.getTime+"0000").toLong
      // 添加 CommentCF
      put.addColumn(Bytes.toBytes(getCommentCF),Bytes.toBytes("dbEffectTime"),ts,Bytes.toBytes(dbEffectTime))
      put.addColumn(Bytes.toBytes(getCommentCF),Bytes.toBytes("endTime"),ts,Bytes.toBytes(endTime))
      put.addColumn(Bytes.toBytes(getCommentCF),Bytes.toBytes("processTime"),ts,Bytes.toBytes((endTime-dbEffectTime)))
      //将数据改为非历史数据（数据初始化时，导入的数据is_his为true，后面进来的数据都为false，做下区分）
      put.addColumn(Bytes.toBytes(getCommentCF),Bytes.toBytes("is_his"),ts,Bytes.toBytes("false"))


      HBasePut(tableName,
        put,
        MongoOffset(oplog.getTimestamp.getTime,oplog.getTimestamp.getInc)
      )
    }
  }

  /**
    * 用户查询需要的字段存在这个列族中
    * @return
    */
  private  def getTableCF():String={
    SettingConstant.HBASE_CF.TABLECF
  }

  /**
    * 附加字段存在这个列族
    * @return
    */
  private  def getCommentCF():String={
    SettingConstant.HBASE_CF.COMMENTCF
  }

  private def getTableName(dbName:String,tableName:String):String={
    dbName.toLowerCase()+"_mongo:"+tableName.toLowerCase+"_1500000000"
  }
  
  private def getTs(seconds:Int,inc:Int):Long ={
    //Inc定义为固定3位，如果不够则左边补0
    val incCount = 3
    val resultInc = "0"*(incCount-inc.toString.length)+inc
    (seconds+resultInc).toLong
  }
}
