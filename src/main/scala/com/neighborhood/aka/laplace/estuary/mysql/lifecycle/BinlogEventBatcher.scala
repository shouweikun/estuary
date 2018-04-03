package com.neighborhood.aka.laplace.estuary.mysql.lifecycle

import java.util
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import akka.actor.SupervisorStrategy.Escalate
import akka.actor.{Actor, ActorLogging, ActorRef, AllForOneStrategy, Props}
import akka.pattern._
import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.Column
import com.neighborhood.aka.laplace.estuary.bean.key.BinlogKey
import com.neighborhood.aka.laplace.estuary.bean.support.KafkaMessage
import com.neighborhood.aka.laplace.estuary.core.lifecycle
import com.neighborhood.aka.laplace.estuary.core.lifecycle.Status.Status
import com.neighborhood.aka.laplace.estuary.core.lifecycle.{SourceDataBatcher, Status, _}
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.{CanalEntryJsonHelper, JsonUtil, Mysql2KafkaTaskInfoManager, MysqlBinlogParser}
import com.taobao.tddl.dbsync.binlog.LogEvent

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
  * Created by john_liu on 2018/2/6.
  */
class BinlogEventBatcher(
                          binlogEventSinker: ActorRef,
                          mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,
                          isDdlHandler: Boolean = false
                        ) extends Actor with SourceDataBatcher with ActorLogging {

  implicit val transTaskPool = if (isDdlHandler) Executors.newWorkStealingPool(1) else Executors.newWorkStealingPool(1)

  /**
    * 拼接json用
    */
  private val START_JSON = "{"
  private val END_JSON = "}"
  private val START_ARRAY = "["
  private val END_ARRAY = "]"
  private val KEY_VALUE_SPLIT = ":"
  private val ELEMENT_SPLIT = ","
  private val STRING_CONTAINER = "\""
  /**
    * binlogParser 解析binlog
    */
  lazy val binlogParser: MysqlBinlogParser = mysql2KafkaTaskInfoManager.binlogParser
  /**
    * 执行模式
    */
  val mode = mysql2KafkaTaskInfoManager.taskInfo.isTransactional
  /**
    * 打包的阈值
    */
  val batchThreshold: AtomicLong = if (isDdlHandler) new AtomicLong(1) else mysql2KafkaTaskInfoManager.taskInfo.batchThreshold
  /**
    * 打包的entry
    */
  var entryBatch: List[CanalEntry.Entry] = List.empty
  /**
    * 待保存的BinlogOffset
    */
  var savedOffset: Long = _
  /**
    * 待保存的Binlog文件名称
    */
  var savedJournalName: String = _
  /**
    * 是否计数
    */
  var isCounting = mysql2KafkaTaskInfoManager.taskInfo.isCounting
  /**
    * 是否计时
    */
  var isCosting = mysql2KafkaTaskInfoManager.taskInfo.isCosting

  //offline
  override def receive: Receive = {
    case SyncControllerMessage(msg) => {
      msg match {
        case "start" => {
          batcherChangeStatus(Status.ONLINE)
          context.become(online)
        }
      }
    }
    case BatcherMessage(msg) => {
      msg match {
        case "restart" => {
          self ! SyncControllerMessage("start")
        }
      }
    }
    case entry: CanalEntry.Entry => {


    }
    case x => {

      log.info(s"BinlogBatcher unhandled Message : $x")
    }
  }

  /**
    * online
    */
  def online: Receive = {
    case entry: CanalEntry.Entry => {
      batchAndFlush(entry)()
    }
    case event: LogEvent => {
      tranformAndHandle(event)()
    }
    case SyncControllerMessage(msg) => {
      msg match {
        case "flush" => flush
      }
    }
  }

  /**
    * @param entry canalEntry
    *              打包如果包内数量超过阈值刷新并发送给sinker
    */
  def batchAndFlush(entry: CanalEntry.Entry)(mode: Boolean = this.mode): Unit = {
    if (mode) {
      binlogEventSinker ! entry
    } else {
      entryBatch = entryBatch.+:(entry)
      val theBatchThreshold = batchThreshold.get()
      if (entryBatch.size >= theBatchThreshold) {
        val before = System.currentTimeMillis()
        flush
        val after = System.currentTimeMillis()

      }
    }
  }

  /**
    * 刷新list里的值并发送给sinker
    *
    * @todo 如果json 的开销小，不使用future，其实这个future是不安全的
    */
  def flush = {
    if (!entryBatch.isEmpty) {
      val batch = entryBatch
      val size = batch.size

      def flushData = {
        val before = System.currentTimeMillis()
        val re =
          batch.
            map {
              entry =>
                val entryType = entry.getEntryType //entry类型
              val header = entry.getHeader
                val eventType = header.getEventType //事件类型
              val tempJsonKey = BinlogKey.buildBinlogKey(header)
                //todo 添加tempJsonKey的具体信息
                //tempJsonKey.appName = appName
                //tempJsonKey.appServerIp = appServerIp
                //tempJsonKey.appServerPort = appServerPort
                //tempJsonKey.syncTaskStartTime = getSyncTaskStartTime
                tempJsonKey.syncTaskId = mysql2KafkaTaskInfoManager.taskInfo.syncTaskId
                tempJsonKey.msgSyncStartTime = before
                tempJsonKey.msgSize = entry.getSerializedSize
                //根据entry的类型来执行不同的操作
                entryType match {
                  case CanalEntry.EntryType.TRANSACTIONEND => {
                    //将offset记录下来
                    savedJournalName = header.getLogfileName
                    savedOffset = header.getLogfileOffset
                    BinlogPositionInfo(header.getLogfileName, header.getLogfileOffset)
                  }
                  case CanalEntry.EntryType.ROWDATA => {
                    eventType match {
                      //DML操作都执行tranformDMLtoJson这个方法
                      case CanalEntry.EventType.DELETE => tranformDMLtoJson(entry, tempJsonKey, "DELETE")
                      case CanalEntry.EventType.INSERT => tranformDMLtoJson(entry, tempJsonKey, "INSERT")
                      case CanalEntry.EventType.UPDATE => tranformDMLtoJson(entry, tempJsonKey, "UPDATE")
                      //DDL操作直接将entry变为json
                      case CanalEntry.EventType.ALTER => {
                        transferDDltoJson(tempJsonKey, entry, header.getLogfileName, header.getLogfileOffset, before)
                      }
                      case CanalEntry.EventType.CREATE => {
                        transferDDltoJson(tempJsonKey, entry, header.getLogfileName, header.getLogfileOffset, before)
                        val theAfter = System.currentTimeMillis()
                        tempJsonKey.setMsgSyncEndTime(theAfter)
                        tempJsonKey.setMsgSyncUsedTime(theAfter - before)
                      }
                      case x => {

                        log.warning(s"unsupported EntryType:$x")
                      }

                    }
                  }
                }

            }
        val after = System.currentTimeMillis()
        //log.info(s"batcher json化 用了${after - before}")
        if (isCounting) mysql2KafkaTaskInfoManager.batchCount.getAndAdd(size)
        if (isCosting) mysql2KafkaTaskInfoManager.powerAdapter match {
          case Some(x) => x ! BatcherMessage(s"${after - before}")
          case _ => log.warning("powerAdapter not exist")
        }
        re
      }

      if (isDdlHandler) binlogEventSinker ! flushData else Future(flushData).pipeTo(binlogEventSinker)
      entryBatch = List.empty
    } else {

    }
  }

  /**
    * entry 解码
    */
  private def transferEvent(event: LogEvent): Option[CanalEntry.Entry] = {
    binlogParser.parseAndProfilingIfNecessary(event, false)
  }

  /**
    * entry 解码并处理
    */
  @deprecated
  def tranformAndHandle(event: LogEvent)(mode: Boolean = this.mode) = {
    val entry = transferEvent(event)
    (entry.isDefined, mode) match {
      //transactional模式
      case (true, true) => {
        log.info(s"${entry.get.getHeader.getLogfileName}-${entry.get.getHeader.getLogfileOffset} send to sinker with transactional mode")
        binlogEventSinker ! entry.get
      }
      //concurrent模式
      case (true, false) => {
        log.info(s"${entry.get.getHeader.getLogfileName}-${entry.get.getHeader.getLogfileOffset} send to sinker with concurrent mode")
        batchAndFlush(entry.get)()
      }
      case (false, _) => {
        log.warning(s"${event} cannot be resolved")
      }
    }
  }

  /**
    * @param tempJsonKey   BinlogJsonKey
    * @param entry         entry
    * @param logfileName   binlog文件名
    * @param logfileOffset binlog文件偏移量
    * @param before        开始时间
    *                      将DDL类型的CanalEntry 转换成Json
    */
  def transferDDltoJson(tempJsonKey: BinlogKey, entry: CanalEntry.Entry, logfileName: String, logfileOffset: Long, before: Long): KafkaMessage = {
    //让程序知道是DDL
    tempJsonKey.setDbName("DDL")
    val re = new KafkaMessage(tempJsonKey, CanalEntryJsonHelper.entryToJson(entry), logfileName, logfileOffset)
    val theAfter = System.currentTimeMillis()
    tempJsonKey.setMsgSyncEndTime(theAfter)
    tempJsonKey.setMsgSyncUsedTime(theAfter - before)
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
                */
              def rowChangeStr = {
                if (eventString.equals("DELETE")) s"${STRING_CONTAINER}beforeColumns$STRING_CONTAINER$KEY_VALUE_SPLIT$START_ARRAY${
                  (0 until count)
                    .map {
                      columnIndex =>
                        s"${getColumnToJSON(rowData.getBeforeColumns(columnIndex))}$ELEMENT_SPLIT"
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

              val finalDataString = s"${START_JSON}${STRING_CONTAINER}header${STRING_CONTAINER}${KEY_VALUE_SPLIT}${getEntryHeaderJson(entry.getHeader)}${ELEMENT_SPLIT}${STRING_CONTAINER}rowChange${STRING_CONTAINER}${KEY_VALUE_SPLIT}${START_JSON}${STRING_CONTAINER}rowDatas$START_ARRAY" +
                s"${STRING_CONTAINER}${KEY_VALUE_SPLIT}${START_JSON}${rowChangeStr}${END_JSON}${END_JSON}${END_JSON}"
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
  protected def getColumnToJSON(column: CanalEntry.Column) = {
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

  /**
    * @param sb    正在构建的Json
    * @param key   key值
    * @param isEnd 是否是结尾
    *              增加key值
    */
  protected def addKeyValue(sb: StringBuilder, key: String, value: Any, isEnd: Boolean) = {
    sb.append(STRING_CONTAINER).append(key).append(STRING_CONTAINER).append(KEY_VALUE_SPLIT)
    if (value.isInstanceOf[String]) sb.append(STRING_CONTAINER).append(value.asInstanceOf[String].replaceAll("\"", "\\\\\"").replaceAll("[\r\n]+", "")).append(STRING_CONTAINER)
    else if (value.isInstanceOf[Enum[_]]) sb.append(STRING_CONTAINER).append(value).append(STRING_CONTAINER)
    else sb.append(value)
    if (!isEnd) sb.append(ELEMENT_SPLIT)
  }

  /**
    * 错误处理
    */
  override def processError(e: Throwable, message: lifecycle.WorkerMessage): Unit = {
    //batcher 出错不用处理，让他直接崩溃
  }

  override var errorCountThreshold: Int = 3
  override var errorCount: Int = 0

  /**
    * ********************* 状态变化 *******************
    */
  private def changeFunc(status: Status) = TaskManager.changeFunc(status, mysql2KafkaTaskInfoManager)

  private def onChangeFunc = Mysql2KafkaTaskInfoManager.onChangeStatus(mysql2KafkaTaskInfoManager)

  private def batcherChangeStatus(status: Status) = TaskManager.changeStatus(status, changeFunc, onChangeFunc)

  /**
    * ********************* Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    //状态置为offline
    batcherChangeStatus(Status.OFFLINE)


  }

  override def postStop(): Unit = {
    transTaskPool.shutdown()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info("batcher process preRestart")
    batcherChangeStatus(Status.RESTARTING)
    super.preRestart(reason, message)
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info("batcher process postRestart")
    super.postRestart(reason)
  }

  override def supervisorStrategy = {
    AllForOneStrategy() {
      case _ => {
        batcherChangeStatus(Status.ERROR)
        Escalate
      }
    }
  }
}

object BinlogEventBatcher {
  def prop(binlogEventSinker: ActorRef, mysql2KafkaTaskInfoManager: Mysql2KafkaTaskInfoManager,isDdlHandler: Boolean = false): Props = Props(new BinlogEventBatcher(binlogEventSinker, mysql2KafkaTaskInfoManager))
}


