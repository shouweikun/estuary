package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import java.util.concurrent.ConcurrentHashMap

import com.neighborhood.aka.laplace.estuary.bean.exception.schema.NoCorrespondingTableIdException
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.SchemaEntry.MysqlConSchemaEntry
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.TableSchemaVersionCache.{MysqlSchemaVersionCollection, MysqlTableNameMappingTableIdEntry}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * Created by john_liu on 2018/6/3.
  */
class TableSchemaVersionCache(
                               val dbName: String,
                               val syncTaskId: String
                             ) {

  lazy val log = LoggerFactory.getLogger(s"TableSchemaVersionCache-$dbName-$syncTaskId")

  /**
    * key:DbName
    * value:Ca
    */
  val tableSchemaVersionMap: ConcurrentHashMap[String, mutable.Map[Int, MysqlSchemaVersionCollection]] = new ConcurrentHashMap[String, mutable.Map[Int, MysqlSchemaVersionCollection]]()
  /**
    * key:tableName
    * value:tableId
    */
  val tableNameMappingTableIdMap: ConcurrentHashMap[String, List[MysqlTableNameMappingTableIdEntry]] = new ConcurrentHashMap[String, List[MysqlTableNameMappingTableIdEntry]]()
  init

  def init() = {}

  /**
    *
    * 【专门】更新缓存的schema信息
    * 0.数据合法性校验
    * 1.获取【当前条件下】这个tableName对应的tableId
    * 2.添加缓存
    *   2.1 如果数据库中有这个table Id
    *       2.1.1 获取原来的schemas
    *       2.1.2 计算出最新的version版本
    *       2.1.3 更新回tableSchemaVersionMap
    *   2.2 如果没有这个table id
    *       2.2.1 添加到tableSchemaVersionMap中，此时版本是0
    *
    * @param tableName
    * @param schema
    * @param binlogPositionInfo
    */
  def upsertSchemas(
                     tableName: String,
                     schema: List[MysqlConSchemaEntry],
                     binlogPositionInfo: BinlogPositionInfo = BinlogPositionInfo("mysql-bin.000001", 4, 0)
                   ): Unit = {
    if (tableName.isEmpty) {
      log.warn(s"tableName is empty,dbName:$dbName,id:$syncTaskId");
      return;
    }
    if (schema.isEmpty) {
      log.warn(s"schema info is empty,dbName:$dbName,id:$syncTaskId");
      return;
    }

    val theTableId = findTableId(tableName, binlogPositionInfo)
    if (tableSchemaVersionMap.containsKey(theTableId)) {
      lazy val oldSchemas = tableSchemaVersionMap.get(theTableId)
      lazy val newVersion = oldSchemas.keySet.max + 1
      oldSchemas
      lazy val newSchemas = oldSchemas.+(newVersion -> MysqlSchemaVersionCollection(theTableId, tableName, newVersion, binlogPositionInfo, schema))
      tableSchemaVersionMap.put(theTableId, newSchemas)
    } else {
      tableSchemaVersionMap.put(theTableId, mutable.Map(0 -> MysqlSchemaVersionCollection(theTableId, tableName, 0, binlogPositionInfo, schema)))
    }
  }

  /**
    * 更新tableNameMappingTableIdMap
    *
    * 1.如果tableName存在
    *   1.1 更新之
    * 2.否则添加之
    *
    * @param tableName
    * @param tableId
    * @param binlogPositionInfo
    * @return
    */
  private def updateTableNameMappingTableIdMap(tableName: String, tableId: String, binlogPositionInfo: BinlogPositionInfo) = {
    if (tableNameMappingTableIdMap.containsKey(tableName)) {
      lazy val newTableIds = tableNameMappingTableIdMap.get(tableName).:+(MysqlTableNameMappingTableIdEntry(tableId, binlogPositionInfo))
      tableNameMappingTableIdMap.put(tableName, newTableIds)
    } else {
      tableNameMappingTableIdMap.put(tableName, List(MysqlTableNameMappingTableIdEntry(tableId, binlogPositionInfo)))
    }
  }

  /**
    * 根据当前binlog 的时间戳/binlogPostion(未来支持)
    * 来寻找$tableName 对应的 tableId
    *
    * 方法逻辑见内部的loopFind
    *
    * @param tableName
    * @param binlogPositionInfo
    * @return
    */
  def findTableId(tableName: String, binlogPositionInfo: BinlogPositionInfo): String = {
    //todo 检查集合是不是有顺序
    //todo binlogOffset寻址
    lazy val list = tableNameMappingTableIdMap.get(tableName)
    loopFindId(list)

    /**
      * case的三种情况
      * 1.只剩最后的一个元素 => 返回最后一个元素的tableId
      * 2.迭代中途
      *   2.1 head 的时间戳小于传入的时间戳
      *   2.2 否则继续迭代
      * 3.空列表 Nil
      * 抛出:
      *
      * @throws NoCorrespondingTableIdException
      * @param remain
      * @return tableId
      */
    @tailrec
    def loopFindId(remain: => List[MysqlTableNameMappingTableIdEntry]): String = {
      remain match {
        case hd :: Nil => hd.tableId
        case hd :: tl => if (hd.binlogInfo.timestamp < binlogPositionInfo.timestamp) hd.tableId else loopFindId(tl)
        case Nil => throw new NoCorrespondingTableIdException(
          {
            lazy val message = s"cannot find tableId at tableName when loop Find table Id,the list has been run out ,pls check!!!:${dbName}.${tableName},binlogInfo:$binlogPositionInfo,id:${syncTaskId}"
            log.error(message)
            message
          }
        )
      }
    }
  }

  def getLatestVersion(table: String): Long = {
    ???
  }

  def handlerDdl4updateSchema: Unit = ???

  /**
    * 当发生Alter语句时
    * 执行以下操作:
    * 1.表改名
    *   1.1 找出Alter前的tableName
    *   1.2 获得Alter前的tableId
    *   1.3 更新TableNameMappingTableIdMap
    *   1.4 调用upsertSchemas
    */
  def onAlterTable: Unit = ???

  def onCreateTable: Unit = ???

  def getCorespondingVersionSchema(tableName: String, binlogPositionInfo: BinlogPositionInfo): MysqlSchemaVersionCollection = {
    lazy val tableId = findTableId(tableName, binlogPositionInfo)
    lazy val tableVersionMap = tableSchemaVersionMap.get(tableId).toList.map(_._2)

    //todo offset寻址
    @tailrec
    def loopFindVersion(remain: List[MysqlSchemaVersionCollection]): MysqlSchemaVersionCollection = {
      remain match {
        case hd :: Nil => hd
        case hd :: tl => if (hd.binlogInfo.timestamp < binlogPositionInfo.timestamp) hd else loopFindVersion(tl)
        case Nil => null

      }
    }

    loopFindVersion(tableVersionMap)
  }
}

object TableSchemaVersionCache {

  case class MysqlSchemaVersionCollection(
                                           tableId: String,
                                           tableName: String,
                                           version: Int,
                                           binlogInfo: BinlogPositionInfo,
                                           schemas: List[MysqlConSchemaEntry]
                                         ) {
    lazy val fieldSize = schemas.size
  }


  case class MysqlTableNameMappingTableIdEntry(
                                                tableId: String,
                                                binlogInfo: BinlogPositionInfo
                                              )


}
