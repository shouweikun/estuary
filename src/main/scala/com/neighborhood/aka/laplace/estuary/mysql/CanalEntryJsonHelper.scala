package com.neighborhood.aka.laplace.estuary.mysql

import com.alibaba.otter.canal.protocol.CanalEntry
import com.google.protobuf.InvalidProtocolBufferException
import com.googlecode.protobuf.format.JsonFormat
import com.neighborhood.aka.laplace.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage

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

  def dummyKafkaMessage(dbName: String): KafkaMessage = {
    val tableName = "daas_heartbeats_check"
    val jsonKey = new BinlogKey
    val jsonValue =
      s"""{
         |	"header": {
         |		"version": 1,
         |		"logfileName": "mysql-bin.000000",
         |		"logfileOffset": 4,
         |		"serverId": 0,
         |		"serverenCode": "UTF-8",
         |		"executeTime":
         |		"sourceType": "MYSQL",
         |		"schemaName": "$dbName",
         |		"tableName": "$tableName",
         |		"eventLength": 651,
         |		"eventType": "INSERT"
         |	},
         |	"rowChange": {
         |		"rowDatas": [{
         |			"afterColumns": [{
         |				"sqlType": -5,
         |				"isNull": false,
         |				"mysqlType": "bigint(20) unsigned",
         |				"name": "id",
         |				"isKey": true,
         |				"index": 0,
         |				"updated": true,
         |				"value": "${System.currentTimeMillis()}"
         |			} ]
         |		}]
         |	}
         |}"""
    jsonKey.setDbName(dbName)
    jsonKey.setTableName(tableName)
    new KafkaMessage(jsonKey, jsonValue)
  }

  def headerToJson(obj: CanalEntry.Header): String = jsonFormat.printToString(obj)
}
