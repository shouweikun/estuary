package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.batch.mappingFormat

import com.neighborhood.aka.laplace.estuary.bean.support.HBasePut
import com.neighborhood.aka.laplace.estuary.mongo.{SettingConstant, lifecycle}
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoConnection, MongoOffset}
import com.neighborhood.aka.laplace.estuary.mongo.util.{MD5Utils, MongoDocumentToJson}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by john_liu on 2019/3/15.
  *
  * @author neighborhood.aka.laplace
  */
class OplogHBaseMappingFormat(
                               override val mongoConnection: MongoConnection,
                               override val syncTaskId: String,
                               override val mongoDocumentToJson: MongoDocumentToJson
                             ) extends OplogMappingFormat[HBasePut[MongoOffset]] {


  override def transform(x: lifecycle.OplogClassifier): HBasePut[MongoOffset] = {
    val oplog = x.toOplog
    val tableName = getTableName(oplog.getDbName, oplog.getTableName)
    val docOption = getRealDoc(oplog)
    lazy val mongoOffset = MongoOffset(oplog.getTimestamp.getTime, oplog.getTimestamp.getInc)
    if (docOption.isEmpty) {
      HBasePut.abnormal(tableName, mongoOffset)
    } else {
      val doc = docOption.get
      val value = getJsonValue(doc)
      val put = new Put(Bytes.toBytes(MD5Utils.md5(doc.get("_id").toString)))
      val endTime = System.currentTimeMillis()
      val ts = getTs(oplog.getTimestamp.getTime, oplog.getTimestamp.getInc)
      val dbEffectTime = (oplog.getTimestamp.getTime + "000").toLong

      put.addColumn(Bytes.toBytes(getTableCF), Bytes.toBytes("0"),ts, Bytes.toBytes(value))
      put.addColumn(Bytes.toBytes(getTableCF), Bytes.toBytes("1"),ts, Bytes.toBytes(doc.get("_id").toString))

      // 添加 CommentCF
      put.addColumn(Bytes.toBytes(getCommentCF), Bytes.toBytes("dbEffectTime"), ts, Bytes.toBytes(dbEffectTime.toString))
      put.addColumn(Bytes.toBytes(getCommentCF), Bytes.toBytes("endTime"), ts, Bytes.toBytes(endTime.toString))
      put.addColumn(Bytes.toBytes(getCommentCF), Bytes.toBytes("processTime"), ts, Bytes.toBytes((endTime - dbEffectTime).toString))
      //将数据改为非历史数据（数据初始化时，导入的数据is_his为true，后面进来的数据都为false，做下区分）
      put.addColumn(Bytes.toBytes(getCommentCF), Bytes.toBytes("is_his"), ts, Bytes.toBytes("false"))


      HBasePut(
        tableName,
        put,
        mongoOffset
      )
    }
  }

  /**
    * 用户查询需要的字段存在这个列族中
    *
    * @return
    */
  private def getTableCF(): String = {
    SettingConstant.HBASE_CF.TABLECF
  }

  /**
    * 附加字段存在这个列族
    *
    * @return
    */
  private def getCommentCF(): String = {
    SettingConstant.HBASE_CF.COMMENTCF
  }

  private def getTableName(dbName: String, tableName: String): String = {
        dbName.toLowerCase()+"_mongo:"+tableName.toLowerCase+"_1500000000"
//    "liushouwei_t_1"
  }

  private def getTs(seconds: Int, inc: Int): Long = {
    //Inc定义为固定3位，如果不够则左边补0
    val incCount = 3
    val resultInc = "0" * (incCount - inc.toString.length) + inc
    (seconds + resultInc).toLong
  }
}
