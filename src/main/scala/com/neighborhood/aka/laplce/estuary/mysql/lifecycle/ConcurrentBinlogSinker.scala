package com.neighborhood.aka.laplce.estuary.mysql.lifecycle

import java.util
import java.util.{HashMap, Map}

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props}
import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.Column
import com.alibaba.otter.canal.protocol.position.{EntryPosition, LogPosition}
import com.neighborhood.aka.laplce.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplce.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplce.estuary.core.lifecycle
import com.neighborhood.aka.laplce.estuary.core.lifecycle.{SinkerMessage, SourceDataSinker, Status, SyncControllerMessage}
import com.neighborhood.aka.laplce.estuary.core.utils.JsonUtil
import com.neighborhood.aka.laplce.estuary.mysql.{CanalEntryJsonHelper, Mysql2KafkaTaskInfoManager}
import org.I0Itec.zkclient.exception.ZkTimeoutException

import scala.util.{Failure, Success, Try}

/**
  * Created by john_liu on 2018/2/9.
  */
class ConcurrentBinlogSinker(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager, binlogPositionRecorder: ActorRef) extends Actor with SourceDataSinker {

  private val START_JSON = "{"
  private val END_JSON = "}"
  private val START_ARRAY = "["
  private val END_ARRAY = "]"
  private val KEY_VALUE_SPLIT = ":"
  private val ELEMENT_SPLIT = ","
  private val STRING_CONTAINER = "\""

  val kafkaSinker = mysql2KafkaTaskInfoManager.kafkaSink
  val logPositionHandler = mysql2KafkaTaskInfoManager.logPositionHandler

  //offline
  override def receive: Receive = {
    case SyncControllerMessage(msg) => {
      msg match {
        case "start" => {
          //online模式
          context.become(online)
          switch2Busy
        }
      }
    }
  }

  def online: Receive = {
    case list: List[CanalEntry.Entry] => {
      //todo log
      //todo 探讨异步写入

      list.map {
              handleSinkTask(_)
            }
      //todo 写入zookeeper

    }
    case x => {
      //todo log
      println(s"sinker online unhandled message $x")

    }
  }

  def handleSinkTask(entry: CanalEntry.Entry) = {
    val header = entry.getHeader
    //todo log
    val tempJsonKey = BinlogKey.buildBinlogKey(header)
    //todo
    //    tempJsonKey.appName = appName
    //    tempJsonKey.appServerIp = appServerIp
    //    tempJsonKey.appServerPort = appServerPort
    //    tempJsonKey.syncTaskStartTime = getSyncTaskStartTime
    //    tempJsonKey.syncTaskId = syncTaskConf.getSyncTaskId
    if (entry.getEntryType.equals(CanalEntry.EntryType.TRANSACTIONEND)) {
      //todo 可以在这保存offset
    }
    if (header.getEventType == CanalEntry.EventType.INSERT || header.getEventType == CanalEntry.EventType.DELETE || header.getEventType == CanalEntry.EventType.UPDATE) {
      //todo 通过db和tb 得到topic
      val dbAndTable: String = header.getSchemaName + "." + header.getTableName
      sinkDataIntoKafka(entry, tempJsonKey, dbAndTable)
    }
  }

  def sinkDataIntoKafka(entry: CanalEntry.Entry, tempJsonKey: BinlogKey, dbAndTable: String) = {
    val eventType = entry.getHeader.getEventType match {
      case CanalEntry.EventType.DELETE => "DELETE"
      case _ => "insertOrUpdate"
    }
    tranformDMLtoJson(entry, tempJsonKey, eventType).
      map {
        kafkaMassage =>
          println(kafkaMassage.getJsonValue)
          kafkaSinker.ayncSink(kafkaMassage.getBaseDataJsonKey.asInstanceOf[BinlogKey], kafkaMassage.getJsonValue)
      }
    kafkaSinker.flush

  }

  def tranformDMLtoJson(entry: CanalEntry.Entry, temp: BinlogKey, eventString: String): Array[KafkaMessage] = {
    Try(CanalEntry.RowChange.parseFrom(entry.getStoreValue)) match {
      case Success(rowChange) => {
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

              def rowChangeStr = {
                if (eventString.equals("DELETE")) s"${STRING_CONTAINER}beforeColumns$STRING_CONTAINER$KEY_VALUE_SPLIT${
                  (0 until count)
                    .map {
                      columnIndex =>
                        s"${getColumnToJSON(rowData.getBeforeColumns(columnIndex))}$ELEMENT_SPLIT"
                    }
                    .mkString
                }" else {
                  s"${STRING_CONTAINER}afterColumns$STRING_CONTAINER$KEY_VALUE_SPLIT${
                    (0 until count)
                      .map {
                        columnIndex =>
                          s"${getColumnToJSON(rowData.getAfterColumns(columnIndex))}$ELEMENT_SPLIT"
                      }
                      .mkString
                  }"

                } + s"$END_ARRAY$END_JSON"
              }

              val finalDataString = s"${START_JSON}${STRING_CONTAINER}header${STRING_CONTAINER}${KEY_VALUE_SPLIT}${getEntryHeaderJson(entry.getHeader)}${ELEMENT_SPLIT}${STRING_CONTAINER}rowChange${STRING_CONTAINER}${KEY_VALUE_SPLIT}${START_JSON}${STRING_CONTAINER}rowDatas${STRING_CONTAINER}${KEY_VALUE_SPLIT}${START_ARRAY}${rowChangeStr}${END_ARRAY}${END_JSON}${END_JSON}"
              kafkaMessage.setJsonValue(finalDataString)
              kafkaMessage
          }.toArray
      }
      case Failure(e) => {
        //todo log
        throw new RuntimeException("Error when parse rowchange:" + entry, e)
      }
    }

  }

  protected def getEntryHeaderJson(header: CanalEntry.Header): StringBuilder = {
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

  private def addKeyValue(sb: StringBuilder, key: String, value: Any, isEnd: Boolean) = {
    sb.append(STRING_CONTAINER).append(key).append(STRING_CONTAINER).append(KEY_VALUE_SPLIT)
    if (value.isInstanceOf[String]) sb.append(STRING_CONTAINER).append(value.asInstanceOf[String].replaceAll("\"", "\\\\\"").replaceAll("[\r\n]+", "")).append(STRING_CONTAINER)
    else if (value.isInstanceOf[Enum[_]]) sb.append(STRING_CONTAINER).append(value).append(STRING_CONTAINER)
    else sb.append(value)
    if (!isEnd) sb.append(ELEMENT_SPLIT)
  }

  private def getColumnToJSON(column: CanalEntry.Column) = {
    val columnMap = new util.HashMap[String,AnyRef]
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

  override var errorCountThreshold: Int = 3
  override var errorCount: Int = 0

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {
    //do nothing
  }

  /**
    * ********************* 状态变化 *******************
    */

  private def switch2Offline = {
    mysql2KafkaTaskInfoManager.sinkerStatus = Status.OFFLINE
  }

  private def switch2Busy = {
    mysql2KafkaTaskInfoManager.sinkerStatus = Status.BUSY
  }

  private def switch2Error = {
    mysql2KafkaTaskInfoManager.sinkerStatus = Status.ERROR
  }

  private def switch2Free = {
    mysql2KafkaTaskInfoManager.sinkerStatus = Status.FREE
  }

  private def switch2Restarting = {
    mysql2KafkaTaskInfoManager.sinkerStatus = Status.RESTARTING
  }


  /**
    * **************** Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    switch2Offline

  }


  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    context.become(receive)
  }

  override def postRestart(reason: Throwable): Unit = {
    switch2Restarting
    context.parent ! SinkerMessage("restart")
    super.postRestart(reason)
  }

  override def supervisorStrategy = {
    OneForOneStrategy() {
      case e: ZkTimeoutException => {
        Restart
        //todo log
      }
      case e: Exception => {
        switch2Error
        Restart
      }
      case error: Error => Restart
      case _ => Restart
    }
  }
}

object ConcurrentBinlogSinker {
  def prop(mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager, binlogPositionRecorder: ActorRef): Props = {
    Props(new ConcurrentBinlogSinker(mysql2KafkaTaskInfoManager, binlogPositionRecorder))
  }


}