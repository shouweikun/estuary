package com.neighborhood.aka.laplace.estuary.bean.support

import com.neighborhood.aka.laplace.estuary.core.offset.ComparableOffset
import org.apache.hadoop.hbase.client.Put

/**
  * Created by john_liu on 2019/3/14.
  *
  * @author neighborhood.aka.laplace
  */
final case class HdfsMessage[A <: ComparableOffset[A]](
                                                     val dbName:String,
                                                     val tableName: String,
                                                     val value: String,
                                                     val offset: A,
                                                     val isAbnormal: Boolean = false
                                                   ) {

  val ts = System.currentTimeMillis()

  override def toString: String =
    s"""{"dbName":"$dbName","tableName":"$tableName","value":$value,"offset":${offset.toString}}""".stripMargin

}

object HdfsMessage {
  def abnormal[A <: ComparableOffset[A]](dbName:String,tableName:String,offset:A): HdfsMessage[A] = HdfsMessage(dbName,tableName,null,offset,true)
}
