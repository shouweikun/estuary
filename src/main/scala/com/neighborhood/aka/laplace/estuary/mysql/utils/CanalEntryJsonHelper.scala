package com.neighborhood.aka.laplace.estuary.mysql.utils

import java.util

import com.alibaba.otter.canal.protocol.CanalEntry
import com.google.protobuf.InvalidProtocolBufferException
import com.googlecode.protobuf.format.JsonFormat
import com.neighborhood.aka.laplace.estuary.bean.key.{BinlogKey, PartitionStrategy}
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.MysqlSchemaHandler

/**
  * Created by john_liu on 2018/2/27.
  */
object CanalEntryJsonHelper {
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

  def dummyKafkaMessage(dbName: String)(implicit mysqlSchemaHandler: Option[MysqlSchemaHandler] = None): KafkaMessage = {
    val tableName = "daas_ds_estuary_heartbeats_check"
    lazy val jsonKey = new BinlogKey
    lazy val time = System.currentTimeMillis()
    lazy val dbId = mysqlSchemaHandler.fold(s"{$dbName}_mysql")(handler => handler.findDbId(dbName))
    lazy val syncTaskId = mysqlSchemaHandler.fold("unKnown")(handler => handler.syncTaskId)
    lazy val dummyPrimaryKey = s"$dbName@$tableName@$time"
    lazy val jsonValue =
      s"""{"header": {"version": 1,"logfileName": "mysql-bin.000000","logfileOffset": ${time / 1000},"serverId": 0,"serverenCode": "UTF-8","executeTime": $time,"sourceType": "MYSQL","schemaName": "$dbName","tableName": "$tableName",	"eventLength": 651,"eventType": "INSERT"	},"rowChange": {"rowDatas": [{"afterColumns": [{"sqlType": -5,"isNull": "false","mysqlType": "bigint(20) unsigned","name": "id","isKey": "true","index": "0",	"updated": "true","value": "${time}"}]}]}}
       """.stripMargin
    jsonKey.setDbName(dbName)
    jsonKey.setHbaseDatabaseName(dbId)
    jsonKey.setTableName(tableName)
    jsonKey.setHbaseTableName(tableName)
    jsonKey.setSyncTaskSequence(1)
    jsonKey.setPartitionStrategy(PartitionStrategy.MOD)
    jsonKey.setFieldMapping(heartsBeatsDummyFieldMapping)
    jsonKey.setAppName(s"heartBeatsData-$dbName")
    jsonKey.setAppServerPort(-1)
    jsonKey.setAppServerIp("-1.-1.-1.-1")
    jsonKey.setSyncTaskId(syncTaskId)
    jsonKey.setPrimaryKeyValue(dummyPrimaryKey)
    new KafkaMessage(jsonKey, jsonValue)
  }

  def headerToJson(obj: CanalEntry.Header): String = jsonFormat.printToString(obj)

  /**
    * 根据mysql类型生成对应的value
    * @param mysqlType mysql类型
    * @param value 原始value
    * @return 返回mysql使用的value
    */
  def getSqlValueByMysqlType(mysqlType: String, value: String): String = {
    mysqlType match {
      case "bit" => value
      case "char" => value
      case "int" => value
      case x if (x.contains("float")) => value
      case x if (x.contains("decimal")) => value
      case x if (x.contains("double")) =>value
      case x if (x.contains("bigInt")) => value
      case _ => s"'$value'"
    }
  }
}
