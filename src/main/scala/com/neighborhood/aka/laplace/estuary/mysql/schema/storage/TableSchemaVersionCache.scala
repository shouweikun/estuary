package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import java.util.concurrent.ConcurrentHashMap

import com.neighborhood.aka.laplace.estuary.bean.exception.schema.{NoCorrespondingSchemaException, NoCorrespondingTableIdException}
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
                               val dbId: String,
                               val syncTaskId: String
                             ) {

  lazy val log = LoggerFactory.getLogger(s"TableSchemaVersionCache-$dbName-$syncTaskId")

  /**
    * key:tableId
    * value:Map
    * {
    * key:Version版本
    * value:MysqlSchemaVersionCollection
    * }
    *
    * 将一对多的tableId和Field Schema 转为一对一的映射
    */
  lazy val tableSchemaVersionMap: ConcurrentHashMap[String, mutable.Map[Int, MysqlSchemaVersionCollection]] = new ConcurrentHashMap[String, mutable.Map[Int, MysqlSchemaVersionCollection]]()
  /**
    * key:tableName
    * value:MysqlTableNameMappingTableIdEntry
    */
  lazy val tableNameMappingTableIdMap: ConcurrentHashMap[String, List[MysqlTableNameMappingTableIdEntry]] = new ConcurrentHashMap[String, List[MysqlTableNameMappingTableIdEntry]]()

  init

  /**
    * 1.初始化 tableSchemaVersionMap
    * 2.初始化 tableNameMappingTableIdMap
    */
  def init() = {
    //todo
  }

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
      lazy val newSchemas = oldSchemas.+(newVersion -> MysqlSchemaVersionCollection(dbId, dbName, theTableId, tableName, newVersion, binlogPositionInfo, schema))
      tableSchemaVersionMap.put(theTableId, newSchemas)
    } else {
      tableSchemaVersionMap.put(theTableId, mutable.Map(0 -> MysqlSchemaVersionCollection(dbId, dbName, theTableId, tableName, 0, binlogPositionInfo, schema)))
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
      * case的二种情况
      * 1.迭代中途
      *   1.1 head 的时间戳小于传入的时间戳
      *   1.2 否则继续迭代
      * 2.空列表 Nil
      * 抛出:
      *
      * @throws NoCorrespondingTableIdException
      * @param remain
      * @return tableId
      */
    @tailrec
    def loopFindId(remain: => List[MysqlTableNameMappingTableIdEntry]): String = {
      remain match {
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


  /**
    * 当发生Alter Table语句时
    * 执行以下操作:
    * 1.表改名
    *   1.1 找出Alter前的tableName
    *   1.2 获得Alter前的tableId
    *   1.3 更新TableNameMappingTableIdMap
    *   1.4 调用upsertSchemas
    */
  def onAlterTable: Unit = ???

  /**
    * 当发生Create Table操作时
    * 执行以下操作:
    * 1.更新TableNameMappingTableIdMap
    * 2.调用upsertSchemas
    */
  def onCreateTable: Unit = ???

  /**
    * 获得匹配的Schema
    * 根据binlogPosition
    * 获得对应的MysqlSchemaVersionCollection
    *
    * 详细的逻辑见内部loopFindVersion
    *
    * @param tableName
    * @param binlogPositionInfo
    * @return
    */
  def getCorespondingVersionSchema(tableName: String, binlogPositionInfo: BinlogPositionInfo): MysqlSchemaVersionCollection = {
    lazy val tableId = findTableId(tableName, binlogPositionInfo)
    lazy val tableVersionMap = tableSchemaVersionMap.get(tableId).toList.map(_._2)

    //todo offset寻址

    /**
      *
      * 1.迭代中途
      *   1.1 head 的时间戳小于传入的时间戳则返回 => head
      *   1.2 否则继续迭代
      * 2.空列表 Nil =>
      * 抛出
      *
      * @throws NoCorrespondingSchemaException
      * @param remain
      * @return MysqlSchemaVersionCollection
      */
    @tailrec
    def loopFindVersion(remain: List[MysqlSchemaVersionCollection]): MysqlSchemaVersionCollection = {
      remain match {
        case hd :: tl => if (hd.binlogInfo.timestamp < binlogPositionInfo.timestamp) hd else loopFindVersion(tl)
        case Nil => throw new NoCorrespondingSchemaException(
          {
            lazy val message = s"cannot find MysqlSchemaVersionCollection at tableName:${dbName}.${tableName} when loop Find Version,the list has been run out ,pls check!!!,binlogInfo:$binlogPositionInfo,id:${syncTaskId}"
            log.error(message)
            message
          }
        )
      }
    }

    loopFindVersion(tableVersionMap)
  }
}

object TableSchemaVersionCache {

  case class MysqlSchemaVersionCollection(dbId: String,
                                          dbName: String,
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
