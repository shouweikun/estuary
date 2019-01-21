package com.neighborhood.aka.laplace.estuary.mysql.utils

import com.alibaba.otter.canal.protocol.CanalEntry
import com.neighborhood.aka.laplace.estuary.bean.exception.batch.StoreValueParseFailureException

import scala.util.Try

/**
  * Created by john_liu on 2018/6/8.
  * CanalEntry 一些判斷和轉化的方法
  */
object CanalEntryTransUtil {

  /**
    * 判斷是rowData
    *
    * @param entry
    * @return
    */
  def isRowData(entry: CanalEntry.Entry): Boolean = isRowData(entry.getEntryType)

  /**
    * 判斷是rowData
    *
    * @param entryType
    * @return
    */
  private def isRowData(entryType: CanalEntry.EntryType): Boolean = entryType.equals(CanalEntry.EntryType.ROWDATA)


  /**
    * 判斷是DML
    *
    * @param entry
    * @return
    */
  def isDml(entry: CanalEntry.Entry): Boolean = isDml(entry.getHeader.getEventType) && isRowData(entry)

  /**
    * 判斷是DML
    * 包含UPDATE DELETE INSERT
    *
    * @param eventType
    * @return
    */
  def isDml(eventType: CanalEntry.EventType): Boolean = {
    lazy val isQualified = eventType.equals(CanalEntry.EventType.UPDATE) ||
      eventType.equals(CanalEntry.EventType.DELETE) ||
      eventType.equals(CanalEntry.EventType.INSERT)
    isQualified
  }

  /**
    * 判定是否是DDL
    * 包含ALTER,CREATE,RENAME,TRUNCATE
    *
    * @param eventType
    * @return
    */
  def isDdl(eventType: CanalEntry.EventType): Boolean = {


    eventType.equals(CanalEntry.EventType.ALTER) ||
      eventType.equals(CanalEntry.EventType.CREATE) ||
               eventType.equals(CanalEntry.EventType.ERASE) ||
      eventType.equals(CanalEntry.EventType.RENAME) ||
      eventType.equals(CanalEntry.EventType.TRUNCATE)


  }

  /**
    * 判定是否是ddl
    * 包含ALTER,CREATE,RENAME
    *
    * @return
    */
  def isDdl(entry: CanalEntry.Entry): Boolean = isDdl(entry.getHeader.getEventType) && isRowData(entry)

  /**
    * 判斷是否是事務結束位置
    *
    * @param entry
    * @return
    */
  def isTransactionEnd(entry: CanalEntry.Entry): Boolean = isTransactionEnd(entry.getEntryType)

  /**
    * 判断是否是事务结束位置
    *
    * @param entryType
    * @return
    */
  def isTransactionEnd(entryType: CanalEntry.EntryType): Boolean = entryType.equals(CanalEntry.EntryType.TRANSACTIONEND)

  /**
    * 解析Entry
    *
    * @param entry
    * @param syncTaskId
    * @return
    */
  def parseStoreValue(entry: CanalEntry.Entry)(syncTaskId: String = "unknown") = {
    def parseError = {
      throw new StoreValueParseFailureException(s"parse row data:${CanalEntryJsonHelper.headerToJson(entry.getHeader)} error,id:${syncTaskId}")
    }

    Try(CanalEntry.RowChange.parseFrom(entry.getStoreValue)).getOrElse(parseError)
  }
}
