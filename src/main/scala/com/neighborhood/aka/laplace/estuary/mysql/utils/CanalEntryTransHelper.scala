package com.neighborhood.aka.laplace.estuary.mysql.utils

import java.util

import com.alibaba.otter.canal.protocol.CanalEntry
import com.google.protobuf.InvalidProtocolBufferException
import com.googlecode.protobuf.format.JsonFormat

/**
  * Created by john_liu on 2018/2/27.
  */
object CanalEntryTransHelper {
  private val jsonFormat = new JsonFormat


  /**
    * 这个是个不良好的设计，
    * 但是在对象中缓存这个FieldMapping 保证每次使用不需要在造mapping
    */
  private lazy val heartsBeatsDummyFieldMapping = {
    lazy val re = new util.HashMap[String, String]()
    re.put("name", "0")
    re
  }

  /**
    * entry 转换为Json
    *
    * @param entry entry
    * @return Json
    */
  def entryToJson(entry: CanalEntry.Entry): String = {
    val sb = new StringBuilder(entry.getSerializedSize + 2048)
    sb.append("{\"header\":")
    sb.append(jsonFormat.printToString(entry.getHeader))
    sb.append(",\"entryType\":\"")
    sb.append(entry.getEntryType.name)
    try {
      val rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue)
      sb.append("\",\"rowChange\":")
      sb.append(jsonFormat.printToString(rowChange))
    } catch {
      case e: InvalidProtocolBufferException =>
      //todo log
    }
    sb.append("}")
    sb.toString
  }


  def headerToJson(obj: CanalEntry.Header): String = jsonFormat.printToString(obj)

  /**
    * 根据mysql类型生成对应的value
    *
    * @param mysqlType mysql类型
    * @param value     原始value
    * @return 返回mysql使用的value
    */
  def getSqlValueByMysqlType(mysqlType: String, value: String): String = {
    if(value.isEmpty) """"""""
    else mysqlType.toLowerCase match {
      case x if (x.startsWith("char")) => value
      case x if (x.contains("int")) => value
      case x if (x.contains("bit")) => value
      case x if (x.contains("float")) => value
      case x if (x.contains("double")) => value
      case x if (x.contains("bigint")) => value
      case _ => s""""${value.replaceAll("\"","""\\"""")}"""" //add escape char handler
    }
  }

}
