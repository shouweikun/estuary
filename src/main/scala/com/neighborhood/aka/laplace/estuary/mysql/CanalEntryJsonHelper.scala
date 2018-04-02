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
  def dummyDataJson:String = {
    val tableName = "daas_heartbeats_check"
    s"""{
      |	"header": {
      |		"version": 1,
      |		"logfileName": "mysql-bin.000000",
      |		"logfileOffset": 4,
      |		"serverId": 0,
      |		"serverenCode": "UTF-8",
      |		"executeTime":
      |		"sourceType": "MYSQL",
      |		"schemaName": "qianjin",
      |		"tableName": "biz_event_data",
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
      |				"value": "26772383"
      |			}, {
      |				"sqlType": 12,
      |				"isNull": false,
      |				"mysqlType": "varchar(50)",
      |				"name": "event_id",
      |				"isKey": false,
      |				"index": 1,
      |				"updated": true,
      |				"value": "3b017d17-1d1d-4c86-8fd8-c0f1b9ab907b"
      |			}, {
      |				"sqlType": 12,
      |				"isNull": false,
      |				"mysqlType": "varchar(50)",
      |				"name": "data_id",
      |				"isKey": false,
      |				"index": 2,
      |				"updated": true,
      |				"value": "f7ff228a-e388-4268-beec-2d02093caf9a"
      |			}, {
      |				"sqlType": 4,
      |				"isNull": false,
      |				"mysqlType": "int(10) unsigned",
      |				"name": "data_type",
      |				"isKey": false,
      |				"index": 3,
      |				"updated": true,
      |				"value": "1"
      |			}, {
      |				"sqlType": 12,
      |				"isNull": false,
      |				"mysqlType": "varchar(3000)",
      |				"name": "data_content",
      |				"isKey": false,
      |				"index": 4,
      |				"updated": true,
      |				"value": "{\"id\":null,\"llrId\":92937330,\"loanId\":226481,\"phase\":18,\"lenderId\":6478169,\"lenderName\":\"iqj4e73b366a0\",\"buyShares\":1.0,\"amount\":50.0,\"available\":46.76,\"available_money\":null,\"award\":0.0,\"buyLoanType\":13,\"planId\":5012,\"planSubAccountId\":1853397,\"createTime\":null,\"updateTime\":null,\"updateUserId\":0,\"version\":0,\"investmentChannel\":0,\"gainProcess\":0,\"status\":0,\"planName\":\"KE17J24B\",\"transferFreezeShare\":null,\"avaliableMoney\":null,\"freezeMoney\":null,\"originRegistMoney\":null}"
      |			}, {
      |				"sqlType": 12,
      |				"isNull": false,
      |				"mysqlType": "varchar(200)",
      |				"name": "data_content_type",
      |				"isKey": false,
      |				"index": 5,
      |				"updated": true,
      |				"value": "com.caikee.core.model.HxLoanLendRecord"
      |			}, {
      |				"sqlType": 4,
      |				"isNull": false,
      |				"mysqlType": "int(10) unsigned zerofill",
      |				"name": "data_content_order",
      |				"isKey": false,
      |				"index": 6,
      |				"updated": true,
      |				"value": "1"
      |			}, {
      |				"sqlType": 4,
      |				"isNull": false,
      |				"mysqlType": "int(10) unsigned",
      |				"name": "version",
      |				"isKey": false,
      |				"index": 7,
      |				"updated": true,
      |				"value": "1"
      |			}, {
      |				"sqlType": 93,
      |				"isNull": false,
      |				"mysqlType": "datetime",
      |				"name": "create_time",
      |				"isKey": false,
      |				"index": 8,
      |				"updated": true,
      |				"value": "2018-04-02 04:39:12"
      |			}, {
      |				"sqlType": 93,
      |				"isNull": false,
      |				"mysqlType": "datetime",
      |				"name": "update_time",
      |				"isKey": false,
      |				"index": 9,
      |				"updated": true,
      |				"value": "2018-04-02 04:39:12"
      |			}, {
      |				"sqlType": 12,
      |				"isNull": false,
      |				"mysqlType": "",
      |				"name": "syncJsonKey",
      |				"isKey": false,
      |				"index": 10,
      |				"updated": false,
      |				"value": "{\"appName\":\"source-to-kafka\",\"appServerIp\":\"192.168.176.11\",\"appServerPort\":8331,\"syncTaskId\":\"MysqlQianJin\",\"syncTaskStartTime\":1522403363344,\"syncTaskSequence\":742283839,\"sourceType\":\"binlog\",\"dbName\":\"qianjin\",\"tableName\":\"biz_event_data\",\"msgUuid\":\"mysql-bin.108389322182359\",\"msgSyncStartTime\":1522640176270,\"msgSyncEndTime\":-1,\"dbEffectTime\":1522615152000,\"msgSyncUsedTime\":0,\"msgSize\":1155,\"kafkaTopic\":\"MysqlIqjQianJin\",\"kafkaPartition\":-1,\"kafkaOffset\":-1,\"eventType\":\"INSERT\",\"dbResId\":null,\"mysqlJournalName\":\"mysql-bin.108389\",\"mysqlPosition\":322182359,\"mysqlTimestamp\":1522615152000,\"serverId\":18,\"savedJournalName\":\"mysql-bin.108389\",\"savedOffset\":321158512}"
      |			}]
      |		}]
      |	}
      |}"""
  }
  def headerToJson(obj: CanalEntry.Header): String = jsonFormat.printToString(obj)
}
