package com.neighborhood.aka.laplace.estuary.mysql

import com.alibaba.otter.canal.protocol.CanalEntry
import com.google.protobuf.InvalidProtocolBufferException
import com.googlecode.protobuf.format.JsonFormat

/**
  * Created by john_liu on 2018/2/27.
  */
object CanalEntryJsonHelper {
  private val jsonFormat = new JsonFormat


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
  def dummyDataJson(dbName:String):String = {
    val tableName = "daas_heartbeats_check"
    s"""{
	"header": {
		"version": 1,
		"logfileName": "mysql-bin.000000",
		"logfileOffset": 4,
		"serverId": 0,
		"serverenCode": "UTF-8",
		"executeTime": 0,
		"sourceType": "MYSQL",
		"schemaName": "$dbName",
		"tableName": "$tableName",
		"eventLength": 0,
		"eventType": "INSERT"
	},
	"rowChange": {
		"rowDatas": [{
			"afterColumns": [{
				"index": "0",
				"isKey": "false",
				"isNull": "false",
				"mysqlType": "varchar(255)",
				"name": "cif_check",
				"sqlType": "12",
				"updated": "true",
				"value": "${System.currentTimeMillis()}"
			}]
		}]
	}
}"""
  }
  def headerToJson(obj: CanalEntry.Header): String = jsonFormat.printToString(obj)
}
