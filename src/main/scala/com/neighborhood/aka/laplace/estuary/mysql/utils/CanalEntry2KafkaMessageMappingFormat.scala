package com.neighborhood.aka.laplace.estuary.mysql.utils

import java.util

import akka.actor.{Actor, ActorLogging}
import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.Column
import com.google.protobuf.InvalidProtocolBufferException
import com.googlecode.protobuf.format.JsonFormat
import com.neighborhood.aka.laplace.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.trans.MappingFormat

import scala.util.{Failure, Success, Try}

/**
  * Created by john_liu on 2018/5/1.
  */
trait CanalEntry2KafkaMessageMappingFormat extends MappingFormat[CanalEntry.Entry, Array[KafkaMessage]] {
  self: Actor with ActorLogging =>

  val syncTaskId: String
  val config = context.system.settings.config
  private val jsonFormat = new JsonFormat
  lazy val appName = if (config.hasPath("app.name")) config.getString("app.name") else ""
  lazy val appServerIp = if (config.hasPath("app.server.ip")) config.getString("app.server.ip") else ""
  lazy val appServerPort = if (config.hasPath("app.server.port")) config.getInt("app.server.port") else -1

  override def transform(entry: CanalEntry.Entry): Array[KafkaMessage] = {
    lazy val before = System.currentTimeMillis()
    val header = entry.getHeader
    val eventType = header.getEventType
    val tempJsonKey = BinlogKey.buildBinlogKey(header)
    tempJsonKey.setAppName(appName)
    tempJsonKey.setAppServerIp(appServerIp)
    tempJsonKey.setAppServerPort(appServerPort)
    tempJsonKey.setSyncTaskId(syncTaskId)
    tempJsonKey.setMsgSyncStartTime(before)
    eventType match {
      case CanalEntry.EventType.DELETE => tranformDMLtoJson(entry, tempJsonKey, "DELETE")
      case CanalEntry.EventType.INSERT => tranformDMLtoJson(entry, tempJsonKey, "INSERT")
      case CanalEntry.EventType.UPDATE => tranformDMLtoJson(entry, tempJsonKey, "UPDATE")
      //DDL操作直接将entry变为json,目前只处理Alter
      case CanalEntry.EventType.ALTER => {
        Array(transferDDltoJson(tempJsonKey, entry))
      }
    }
  }

  /**
    * @param tempJsonKey BinlogJsonKey
    * @param entry       entry
    *                    将DDL类型的CanalEntry 转换成Json
    */
  def transferDDltoJson(tempJsonKey: BinlogKey, entry: CanalEntry.Entry): KafkaMessage = {
    ???
    //让程序知道是DDL
    tempJsonKey.setDdl(true)
    log.info(s"batch ddl ${entryToJson(entry)},id:$syncTaskId")
    val re = new KafkaMessage(tempJsonKey, entryToJson(entry))
    val theAfter = System.currentTimeMillis()
    tempJsonKey.setMsgSyncEndTime(theAfter)
    tempJsonKey.setMsgSyncUsedTime(theAfter - tempJsonKey.getMsgSyncStartTime)
    re
  }

  /**
    * @param entry       entry
    * @param temp        binlogKey
    * @param eventString 事件类型
    *                    将DML类型的CanalEntry 转换成Json
    */
  def tranformDMLtoJson(entry: CanalEntry.Entry, temp: BinlogKey, eventString: String): Array[KafkaMessage] = {

    val re = Try(CanalEntry.RowChange.parseFrom(entry.getStoreValue)) match {
      case Success(rowChange) => {
        val a = rowChange.getRowDatasCount
        (0 until rowChange.getRowDatasCount)
          .map {
            index =>
              val rowData = rowChange.getRowDatas(index)

              val count = if (eventString.equals("DELETE")) rowData.getBeforeColumnsCount else rowData.getAfterColumnsCount
              val jsonKeyColumnBuilder: Column.Builder
              = CanalEntry.Column.newBuilder
              jsonKeyColumnBuilder.setSqlType(12) //string 类型.
              jsonKeyColumnBuilder.setName("syncJsonKey")
              val jsonKey = temp.clone().asInstanceOf[BinlogKey]

              //            todo
              //  jsonKey.syncTaskSequence = addAndGetSyncTaskSequence
              val kafkaMessage = new KafkaMessage
              kafkaMessage.setBaseDataJsonKey(jsonKey)
              jsonKeyColumnBuilder.setIndex(count)
              jsonKeyColumnBuilder.setValue(JsonUtil.serialize(jsonKey))

              /**
                * 构造rowChange对应部分的json
                * 同时将主键值保存下来
                *
                */
              def rowChangeStr = {

                if (eventString.equals("DELETE")) s"${STRING_CONTAINER}beforeColumns$STRING_CONTAINER$KEY_VALUE_SPLIT$START_ARRAY${
                  (0 until count)
                    .map {
                      columnIndex =>
                        val column = rowData.getBeforeColumns(columnIndex)
                        //在这里保存下主键的值
                        if (column.getIsKey) temp.setPrimaryKeyValue(temp.getPrimaryKeyValue + "_" + column.getValue)
                        s"${getColumnToJSON(column)}$ELEMENT_SPLIT"
                    }
                    .mkString
                }$END_ARRAY" else {
                  s"${STRING_CONTAINER}afterColumns$STRING_CONTAINER$KEY_VALUE_SPLIT$START_ARRAY${
                    (0 until count)
                      .map {
                        columnIndex =>
                          s"${getColumnToJSON(rowData.getAfterColumns(columnIndex))}$ELEMENT_SPLIT"
                      }
                      .mkString
                  }"

                } + s"${getColumnToJSON(jsonKeyColumnBuilder.build)}$END_ARRAY"
              }

              val finalDataString = s"${START_JSON}${STRING_CONTAINER}header${STRING_CONTAINER}${KEY_VALUE_SPLIT}${getEntryHeaderJson(entry.getHeader)}${ELEMENT_SPLIT}${STRING_CONTAINER}rowChange${STRING_CONTAINER}${KEY_VALUE_SPLIT}${START_JSON}${STRING_CONTAINER}rowDatas" +
                s"${STRING_CONTAINER}${KEY_VALUE_SPLIT}$START_ARRAY${START_JSON}${rowChangeStr}${END_JSON}${END_ARRAY}${END_JSON}${END_JSON}"
              kafkaMessage.setJsonValue(finalDataString)
              kafkaMessage
          }.toArray
      }
      case Failure(e) => {
        log.error("Error when parse rowchange:" + entry, e)
        throw new RuntimeException("Error when parse rowchange:" + entry, e)
      }
    }
    val theAfter = System.currentTimeMillis()
    temp.setMsgSyncEndTime(theAfter)
    temp.setMsgSyncUsedTime(temp.getMsgSyncEndTime - temp.getSyncTaskStartTime)
    //log.info(s"${temp.msgSyncStartTime},${temp.getMsgSyncEndTime-temp.getSyncTaskStartTime}")
    re
  }

  /**
    * @param column CanalEntry.Column
    *               将column转化成Json
    */
  protected def getColumnToJSON(column: CanalEntry.Column): String

  = {
    val columnMap = new util.HashMap[String, AnyRef]
    columnMap.put("index", column.getIndex.toString)
    columnMap.put("sqlType", column.getSqlType.toString)
    columnMap.put("name", column.getName)
    columnMap.put("isKey", column.getIsKey.toString)
    columnMap.put("updated", column.getUpdated.toString)
    columnMap.put("isNull", column.getIsNull.toString)
    val value = column.getValue
    columnMap.put("value", value)
    if (column.getIsNull) columnMap.put("value", null)
    columnMap.put("mysqlType", column.getMysqlType)
    val columnJSON = new JSONObject(columnMap)
    columnJSON.toJSONString
  }

  /**
    * @param header CanalEntryHeader
    *               将entryHeader转换成Json
    */
  protected def getEntryHeaderJson(header: CanalEntry.Header): StringBuilder

  = {
    val sb = new StringBuilder(512)
    sb.append(START_JSON)
    addKeyValue(sb, "version", header.getVersion, false)
    addKeyValue(sb, "logfileName", header.getLogfileName, false)
    addKeyValue(sb, "logfileOffset", header.getLogfileOffset, false)
    addKeyValue(sb, "serverId", header.getServerId, false)
    addKeyValue(sb, "serverenCode", header.getServerenCode, false)
    addKeyValue(sb, "executeTime", header.getExecuteTime, false)
    addKeyValue(sb, "sourceType", header.getSourceType, false)
    addKeyValue(sb, "schemaName", header.getSchemaName, false)
    addKeyValue(sb, "tableName", header.getTableName, false)
    addKeyValue(sb, "eventLength", header.getEventLength, false)
    addKeyValue(sb, "eventType", header.getEventType, true)
    sb.append(END_JSON)
    sb
  }

  /**
    * @param sb    正在构建的Json
    * @param key   key值
    * @param isEnd 是否是结尾
    *              增加key值
    */
  protected def addKeyValue(sb: StringBuilder, key: String, value: Any, isEnd: Boolean)

  = {
    sb.append(STRING_CONTAINER).append(key).append(STRING_CONTAINER).append(KEY_VALUE_SPLIT)
    if (value.isInstanceOf[String]) sb.append(STRING_CONTAINER).append(value.asInstanceOf[String].replaceAll("\"", "\\\\\"").replaceAll("[\r\n]+", "")).append(STRING_CONTAINER)
    else if (value.isInstanceOf[Enum[_]]) sb.append(STRING_CONTAINER).append(value).append(STRING_CONTAINER)
    else sb.append(value)
    if (!isEnd) sb.append(ELEMENT_SPLIT)
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

  def dummyKafkaMessage(dbName: String): KafkaMessage = {
    val tableName = "daas_heartbeats_check"
    lazy val jsonKey = new BinlogKey
    lazy val time = System.currentTimeMillis()
    val jsonValue =
      s"""{"header": {"version": 1,"logfileName": "mysql-bin.000000","logfileOffset": 4,"serverId": 0,"serverenCode": "UTF-8","executeTime": $time,"sourceType": "MYSQL","schemaName": "$dbName","tableName": "$tableName",	"eventLength": 651,"eventType": "INSERT"	},"rowChange": {"rowDatas": [{"afterColumns": [{"sqlType": -5,"isNull": false,"mysqlType": "bigint(20) unsigned","name": "id","isKey": true,"index": 0,	"updated": true,"value": "${time}"}]}]}}
         |
       """.stripMargin
    jsonKey.setDbName(dbName)
    jsonKey.setTableName(tableName)
    new KafkaMessage(jsonKey, jsonValue)
  }

  def headerToJson(obj: CanalEntry.Header): String = jsonFormat.printToString(obj)
}
