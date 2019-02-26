package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.batch.mappings

import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{Column, RowData}
import com.google.protobuf.InvalidProtocolBufferException
import com.googlecode.protobuf.format.JsonFormat
import com.neighborhood.aka.laplace.estuary.bean.exception.batch.UnsupportedEventTypeException
import com.neighborhood.aka.laplace.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.{BinlogPositionInfo, EntryKeyClassifier}
import com.neighborhood.aka.laplace.estuary.mysql.utils.{CanalEntryTransHelper, CanalEntryTransUtil, JsonUtil}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Created by john_liu on 2019/2/19.
  *
  * @author neighborhood.aka.laplace
  */
trait CanalEntry2KafkaMessageMappingFormat extends CanalEntryMappingFormat[KafkaMessage] {

  lazy val log = LoggerFactory.getLogger(classOf[CanalEntry2KafkaMessageMappingFormat])
  /**
    * 这个app的名称
    */
  protected lazy val appName = if (config.hasPath("app.name")) config.getString("app.name") else syncTaskId
  /**
    * 启动任务的server ip
    */
  protected lazy val appServerIp = if (config.hasPath("app.server.ip")) config.getString("app.server.ip") else "unknown ip"
  /**
    * 任务端口
    */
  protected lazy val appServerPort = if (config.hasPath("app.server.port")) config.getInt("app.server.port") else -1
  /**
    * jackson使用
    */
  lazy val jsonFormat = new JsonFormat

  /**
    * 将Id Classifer 转换成KafkaMessage
    * 0.组织各种需要的信息
    * 1.根据header 构建出binlogKey
    * 2.如果是DML
    *   2.1 校验数据信息决定是否放弃
    * 3.set各种公共信息
    * 4.转换对应的KafkaMessage
    *   4.1 DML -> tranformDMLtoJson
    *   4.2 DDL -> transferDDltoJson
    *
    * @param idClassifier
    * @return
    */
  override def transform(idClassifier: EntryKeyClassifier): KafkaMessage = {
    //todo 可以优化
    val before = System.currentTimeMillis()
    lazy val entry = idClassifier.entry
    lazy val rowData = idClassifier.rowData
    lazy val header = entry.getHeader
    lazy val schemaName = header.getSchemaName
    lazy val tableName = header.getTableName
    lazy val journalName = header.getLogfileName
    lazy val executeTime = header.getExecuteTime
    lazy val positionOffset = header.getLogfileOffset
    lazy val binlogPositionInfo = BinlogPositionInfo(journalName, positionOffset, executeTime)
    lazy val eventType = header.getEventType
    //    /**
    //      * field的个数，对于dml rowChange，要判断fieldCount数是否相等
    //      */
    //    lazy val fieldCount = if (eventType == CanalEntry.EventType.DELETE) rowData.getBeforeColumnsCount else rowData.getAfterColumnsCount
    /**
      * fieldList，对于dml rowChange，要判断fieldList是否一致
      */
    lazy val fieldList = {
      lazy val re = if (eventType == CanalEntry.EventType.DELETE) rowData.getBeforeColumnsList.asScala.toList else rowData.getAfterColumnsList.asScala.toList
      re.map(_.getName)
    }

    val tempJsonKey = BinlogKey.buildBinlogKey(header)
    /**
      * 对DML 进行必要的schema信息校验，以判断这条数据是否有效
      */
    if (schemaComponentIsOn && CanalEntryTransUtil.isDml(eventType)) {
      //todo schema 校验
    }

    tempJsonKey.setAppName(appName)
    tempJsonKey.setAppServerIp(appServerIp)
    tempJsonKey.setAppServerPort(appServerPort)
    tempJsonKey.setSyncTaskId(syncTaskId)
    tempJsonKey.setSyncTaskStartTime(syncStartTime)
    tempJsonKey.setMsgSyncStartTime(before)
    tempJsonKey.setPrimaryKeyValue(idClassifier.thePrimaryKey)
    tempJsonKey.setMsgUuid(idClassifier.thePrimaryKey)
    tempJsonKey.setSavedJournalName(journalName)
    tempJsonKey.setSavedOffset(positionOffset)
    tempJsonKey.setDbEffectTime(executeTime)
    tempJsonKey.setSyncTaskSequence(idClassifier.syncSequence)
    tempJsonKey.setPartitionStrategy(partitionStrategy)


    eventType match {
      case CanalEntry.EventType.DELETE => tranformDMLtoJson(header, rowData, tempJsonKey, "DELETE")
      case CanalEntry.EventType.INSERT => tranformDMLtoJson(header, rowData, tempJsonKey, "INSERT")
      case CanalEntry.EventType.UPDATE => tranformDMLtoJson(header, rowData, tempJsonKey, "UPDATE")
      //现在保留Alter的原因是要和老版本兼容
      case CanalEntry.EventType.ALTER => transferDDltoJson(tempJsonKey, entry)
      case CanalEntry.EventType.CREATE => transferDDltoJson(tempJsonKey, entry)
      case CanalEntry.EventType.RENAME => transferDDltoJson(tempJsonKey, entry)
      case CanalEntry.EventType.ERASE => transferDDltoJson(tempJsonKey, entry)
      case CanalEntry.EventType.TRUNCATE => transferDDltoJson(tempJsonKey, entry)
      case _ => throw new UnsupportedEventTypeException(s"unsupported event type:$eventType,entry:${CanalEntryTransHelper.headerToJson(entry.getHeader)},$syncTaskId")
    }

  }

  /**
    * @param tempJsonKey BinlogJsonKey
    * @param entry       entry
    *                    将DDL类型的CanalEntry 转换成Json
    */
  def transferDDltoJson(tempJsonKey: BinlogKey, entry: CanalEntry.Entry): KafkaMessage = {
    //让程序知道是DDL
    tempJsonKey.setDdl(true)
    tempJsonKey.setSyncTaskSequence(0)
    val json = getEntryToJson(entry)
    log.info(s"batch ddl ${json},id:$syncTaskId")
    val re = new KafkaMessage(tempJsonKey, json)
    val theAfter = System.currentTimeMillis()
    tempJsonKey.setMsgSyncEndTime(theAfter)
    tempJsonKey.setMsgSyncUsedTime(theAfter - tempJsonKey.getMsgSyncStartTime)
    re
  }

  /**
    *
    * @param header
    * @param rowData
    * @param temp        binlogKey
    * @param eventString 事件类型
    *                    将DML类型的CanalEntry 转换成Json
    * @return
    */
  def tranformDMLtoJson(header: CanalEntry.Header, rowData: RowData, temp: BinlogKey, eventString: String): KafkaMessage = {


    val count = if (eventString.equals("DELETE")) rowData.getBeforeColumnsCount else rowData.getAfterColumnsCount
    val jsonKeyColumnBuilder: Column.Builder
    = CanalEntry.Column.newBuilder
    jsonKeyColumnBuilder.setSqlType(12) //string 类型.
    jsonKeyColumnBuilder.setName("syncJsonKey")
    val jsonKey = temp.clone().asInstanceOf[BinlogKey]
    //增加event type
    jsonKey.setEventType(eventString)


    /**
      * 构造rowChange对应部分的json
      * 同时将主键值保存下来
      *
      */
    def rowChangeStr = {

      lazy val str = if (eventString.equals("DELETE")) s"${STRING_CONTAINER}beforeColumns$STRING_CONTAINER$KEY_VALUE_SPLIT$START_ARRAY${
        (0 until count)
          .map {
            columnIndex =>
              val column = rowData.getBeforeColumns(columnIndex)
              //在这里保存下主键的值
              s"${getColumnToJSON(column)}"
          }
          .mkString(",")
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
      jsonKeyColumnBuilder.setIndex(count)
      val theAfter = System.currentTimeMillis()
      jsonKey.setMsgSyncEndTime(theAfter)
      jsonKey.setMsgSyncUsedTime(jsonKey.getMsgSyncEndTime - jsonKey.getMsgSyncStartTime)
      jsonKeyColumnBuilder.setValue(JsonUtil.serialize(jsonKey))
      str
    }

    val finalDataString = s"${START_JSON}${STRING_CONTAINER}header${STRING_CONTAINER}${KEY_VALUE_SPLIT}${getEntryHeaderJson(header)}${ELEMENT_SPLIT}${STRING_CONTAINER}rowChange${STRING_CONTAINER}${KEY_VALUE_SPLIT}${START_JSON}${STRING_CONTAINER}rowDatas" +
      s"${STRING_CONTAINER}${KEY_VALUE_SPLIT}$START_ARRAY${START_JSON}${rowChangeStr}${END_JSON}${END_ARRAY}${END_JSON}${END_JSON}"
    KafkaMessage(jsonKey, finalDataString)
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

  def getEntryToJson(entry: CanalEntry.Entry): String
  = {
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
}
