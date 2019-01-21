package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import java.util.concurrent.ConcurrentHashMap

import com.neighborhood.aka.laplace.estuary.bean.exception.schema._
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.DdlType.DdlType
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.SchemaEntry.MysqlConSchemaEntry
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.TableSchemaVersionCache.{MysqlSchemaVersionCollection, MysqlTableNameMappingTableIdEntry}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.Try

/**
  * Created by john_liu on 2018/6/3.
  */
class TableSchemaVersionCache(
                               val dbName: String,
                               val dbId: String,
                               val syncTaskId: String
                             ) {

  private lazy val log = LoggerFactory.getLogger(s"TableSchemaVersionCache-$dbName-$syncTaskId")

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
  private[schema] lazy val tableSchemaVersionMap: ConcurrentHashMap[String, Map[Int, MysqlSchemaVersionCollection]] = new ConcurrentHashMap[String, Map[Int, MysqlSchemaVersionCollection]]()
  /**
    * key:tableName
    * value:MysqlTableNameMappingTableIdEntry
    */
  private[schema] lazy val tableNameMappingTableIdMap: ConcurrentHashMap[String, List[MysqlTableNameMappingTableIdEntry]] = new ConcurrentHashMap[String, List[MysqlTableNameMappingTableIdEntry]]()
  /**
    * key:BinlogPositionInfo
    * value:对应的时间戳
    */
  private[schema] lazy val ddlMap: ConcurrentHashMap[BinlogPositionInfo, String] = new ConcurrentHashMap[BinlogPositionInfo, String]()
  /**
    * 是否初始化
    */
  var isInitialized = false


  /**
    * 1.初始化 tableSchemaVersionMap
    * 2.初始化 tableNameMappingTableIdMap
    * 3.初始化 ddlMap
    * 4.检验schema信息
    */
  def init(list: List[MysqlConSchemaEntry]): Unit = {

    list
      .groupBy(_.tableId)
      .map {
        case (tableId, schemaList) =>
          lazy val versionCollection = schemaList
            .groupBy(_.version)
            .map {
              case (version, versionSchemaList) =>
                lazy val sample = versionSchemaList(0)
                lazy val tableName = sample.tableName.toLowerCase
                lazy val binlogPositionInfo = sample.binlogPositionInfo
                lazy val ddl = sample.ddlSql
                lazy val ddlType = sample.ddlType
                lazy val lastTableId = sample.tableId
                lazy val tableComment = sample.tableComment

                /**
                  * 初始化tableNameMappingTableIdMap
                  */
                def updateTableNameMappingTableIdMap: Unit = {
                  lazy val mysqlTableNameMappingTableIdEntry = MysqlTableNameMappingTableIdEntry(tableId, binlogPositionInfo)
                  if (tableNameMappingTableIdMap.containsKey(tableName)) {
                    lazy val newList = (tableNameMappingTableIdMap.get(tableName).::(mysqlTableNameMappingTableIdEntry)).sortWith {
                      (x1, x2) => BinlogPositionCompare(x1.binlogInfo, x2.binlogInfo)
                    }
                    tableNameMappingTableIdMap.put(tableName, newList)

                  } else {
                    tableNameMappingTableIdMap.put(tableName, List(mysqlTableNameMappingTableIdEntry))
                  }

                }

                /**
                  * 更新ddlMap
                  */
                def updateDdlMap: Unit = {
                  if (version > 0) {
                    if (!ddlMap.containsKey(binlogPositionInfo)) {
                      ddlMap.put(binlogPositionInfo, ddl)
                    }
                  }
                }

                if (sample.schemaId != dbId) throw new SchemaInconsistentException(s"this cache is $dbId,whice is not same as actual dbId:${sample.schemaId},pls check,id:$syncTaskId")
                updateTableNameMappingTableIdMap
                updateDdlMap

                //返回这组kv
                (version -> MysqlSchemaVersionCollection(
                  dbId,
                  dbName,
                  tableId,
                  tableName,
                  version,
                  binlogPositionInfo,
                  versionSchemaList,
                  tableComment,
                  ddl,
                  lastTableId,
                  ddlType
                ))
            }

          /**
            * 初始化tableSchemaVersionMap
            */
          tableSchemaVersionMap.put(tableId, versionCollection)

      }


    schemaVerify
    log.info(s"init TableSchemaVersionCache completed,dbid:$dbId,id:$syncTaskId")
  }

  /**
    * 更新upsert的方法，主要是将ddl处理后整合信息，更新对应的version 逻辑比较复杂
    * 0.判断这个sql是否执行过，如果执行过直接返回None
    * 1.解析ddl sql 得到对应的schemaChange
    * 2.判断，如果是Database级别的操作，返回None
    * 3.判断schemaChange对应的类型，执行对应的操作:
    *   3.1 Alter/Rename 统一对应TableAlter => 逻辑查看 onTableAlter
    *   3.2 Create 对应TableCreate => 逻辑查看 onTableCreate
    *   3.3 Drop 对应tableDrop => 逻辑查看 onTableDrop
    *   3.4 Truncate 对应tableTruncate => 暂时不处理 返回None
    * 4.将change操作过返回的MysqlConSchema 统一进行Version,BinlogPosition,ddl,tableName的修改
    * 5.生成新的MysqlSchemaVersionCollection
    *
    * @param ddlSql
    * @param binlogPositionInfo
    * @param dbName
    * @return
    */
  def updateSchema(
                    ddlSql: String,
                    binlogPositionInfo: BinlogPositionInfo,
                    dbName: String = this.dbName
                  ): Option[MysqlSchemaVersionCollection] = None
//  {
//    lazy val isExistDdl = ddlMap.containsKey(binlogPositionInfo)
//    lazy val schemaChange = SqlParser.parse(ddlSql, dbName)(0)
//    lazy val isDatabaseOperation = (
//      schemaChange.isInstanceOf[DatabaseDrop] ||
//        schemaChange.isInstanceOf[DatabaseCreate] ||
//        schemaChange.isInstanceOf[DatabaseAlter]
//      )
//    //如果是执行过的ddl直接返回None
//    if (isExistDdl) {
//      log.warn(s"this ddl:$ddlSql has been executed, binlogPositionInfo:$binlogPositionInfo,id:$syncTaskId");
//      return None;
//    }
//    //如果没执行过，存一份
//    //    ddlMap.put(binlogPositionInfo, ddlSql)
//
//    if (isDatabaseOperation) {
//      log.warn(s"sql:$ddlSql binlogPositionInfo:$BinlogPositionInfo is ddl Operation,dbName:$dbName,id:$syncTaskId");
//      None
//    } else {
//
//      /**
//        * 当发生Alter Table语句时
//        * 执行以下操作:
//        * 1.表改名
//        *   1.1 找出Alter前的tableName
//        *   1.2 获得Alter前的tableId
//        *   1.3 更新TableNameMappingTableIdMap
//        *   1.4 调用upsertSchemas
//        * checklist：
//        * 字段改名        √
//        * 字段改类型      √
//        * 增加新字段      √
//        * 增加删除过的字段 √
//        * 删除字段        √
//        * 表改名          √
//        */
//      def onAlterTable(x: TableAlter): MysqlSchemaVersionCollection = {
//        log.info(s"ddlSql '$ddlSql' is Alter/Rename ,start onAlterTable,id:$syncTaskId ")
//        lazy val oldTableName: String = x.table.toLowerCase
//        lazy val newTableName: String = if (x.newTableName != null && x.newTableName.trim != null) x.newTableName.toLowerCase else oldTableName
//        lazy val tableId: String = findTableId(oldTableName, binlogPositionInfo)
//        lazy val lastSchema = getCorrespondingVersionSchema(oldTableName, binlogPositionInfo)
//        lazy val tableComment = lastSchema.tableComment
//        lazy val lastTableId = tableId
//        lazy val ddlType = DdlType.AlTER
//        lazy val isOriginal = oldTableName != newTableName
//        lazy val newVersion = lastSchema.version + 1
//
//        lazy val columnModList: List[ColumnMod] = {
//          import scala.collection.JavaConverters._
//          x.columnMods.asScala.toList
//        }
//
//        @tailrec
//        def loopAlter(list: List[ColumnMod], acc: List[MysqlConSchemaEntry]): List[MysqlConSchemaEntry] = {
//          list match {
//            case Nil => acc
//            case hd :: tl => {
//              hd match {
//                case mod: AddColumnMod => {
//                  lazy val columnDef = mod.definition
//                  //todo 拿到默认值
//                  lazy val defaultValue = ""
//                  lazy val columnName = columnDef.getName
//                  lazy val columnType = buildDecimalTypeInfoIfPossible(columnDef)
//                  lazy val columnComment = Option(columnDef.getComment).getOrElse("noComment")
//                  //todo 拿到增加的字段是不是key
//                  lazy val isKey = false
//                  lazy val duplicatedColumn = acc.withFilter(!_.isDeleted).map(_.fieldName).contains(columnName)
//                  //首先确认column 是不是曾经被删除过的，如果是，复用之
//                  lazy val existsDeletedSameRow = acc.withFilter(_.isDeleted).map(row => (row.fieldName.toLowerCase)).contains((columnName.toLowerCase))
//                  // 如果发现重复的column信息，扔出异常人为介入
//                  if (duplicatedColumn) throw new SchemaMatchException(s"error,find duplicatedColumn when add column:$columnName,$columnType,ddl:$ddlSql,binlogPositionInfo:$BinlogPositionInfo,db:$dbName,id:$syncTaskId,pls check at Once!!!")
//                  lazy val nextAcc = if (existsDeletedSameRow) {
//                    //复用信息
//                    def markAsNotDeleted: PartialFunction[MysqlConSchemaEntry, MysqlConSchemaEntry] = new PartialFunction[MysqlConSchemaEntry, MysqlConSchemaEntry] {
//                      override def isDefinedAt(x: MysqlConSchemaEntry): Boolean = true
//
//                      override def apply(x: MysqlConSchemaEntry): MysqlConSchemaEntry = if ((x.fieldName.toLowerCase == columnName.toLowerCase)) x.markNotDeleted(ddlSql, columnComment) else x
//                    }
//
//                    acc.collect(markAsNotDeleted)
//
//                  } else {
//                    //添加一个新的column
//                    val newHBaseIndex = acc.map(_.hbaseIndex).max + 1
//                    acc.::(SchemaEntry.generateMysqlConSchemaEntry(
//                      lastSchema.dbId,
//                      lastSchema.dbName,
//                      lastSchema.tableId,
//                      newTableName,
//                      newHBaseIndex,
//                      columnName,
//                      columnComment, // 暂时拿不到
//                      columnType,
//                      "",
//                      lastSchema.version + 1,
//                      binlogPositionInfo,
//                      isOriginal = isOriginal,
//                      false, //新加的字段不可能delete
//                      isKey,
//                      ddlSql,
//                      tableComment,
//                      lastTableId,
//                      ddlType,
//                      defaultValue
//                    ))
//                  }
//                  loopAlter(tl, nextAcc)
//                }
//                case mod: ChangeColumnMod => {
//                  lazy val oldColumnName = mod.name
//                  lazy val columnDef = mod.definition
//                  lazy val newColumnName = Option(columnDef.getName).getOrElse(oldColumnName)
//                  lazy val newColumnType = Option(columnDef.getType).fold("")(_ => buildDecimalTypeInfoIfPossible(columnDef))
//                  lazy val newColumnComment = Option(columnDef.getComment).getOrElse("no comment")
//                  //如果不含有这个字段的话，扔出异常
//                  if (
//                    !acc
//                      .withFilter(!_.isDeleted)
//                      .map(_.fieldName.toLowerCase)
//                      .contains(oldColumnName.toLowerCase)
//                  ) throw new SchemaMatchException(s"error,cannot find column when change column:{oldColumnName:$oldColumnName,newColumnName:$newColumnName,newColumnType:$newColumnType},ddl:$ddlSql,binlogPositionInfo:$BinlogPositionInfo,db:$dbName,id:$syncTaskId,pls check at Once!!!")
//
//                  def changeColumn: PartialFunction[MysqlConSchemaEntry, MysqlConSchemaEntry] = new PartialFunction[MysqlConSchemaEntry, MysqlConSchemaEntry] {
//                    override def isDefinedAt(x: MysqlConSchemaEntry): Boolean = true
//
//                    override def apply(x: MysqlConSchemaEntry): MysqlConSchemaEntry = if (x.fieldName.toLowerCase == oldColumnName.toLowerCase) x.changeColumnAttributes(newColumnName, newColumnType, newColumnComment) else x
//
//                  }
//
//                  lazy val nextAcc = acc.collect(changeColumn)
//                  loopAlter(tl, nextAcc)
//
//                }
//                case mod: RemoveColumnMod => {
//                  val removedColumnName = mod.name
//                  if (!acc.withFilter(!_.isDeleted).map(_.fieldName.toLowerCase).contains(removedColumnName.toLowerCase)) throw new SchemaMatchException(s"error,cannot find column when remove column:$removedColumnName,ddl:$ddlSql,binlogPositionInfo:$BinlogPositionInfo,db:$dbName,id:$syncTaskId,pls check at Once!!!")
//
//                  def markAsDeleted: PartialFunction[MysqlConSchemaEntry, MysqlConSchemaEntry] = new PartialFunction[MysqlConSchemaEntry, MysqlConSchemaEntry] {
//                    override def isDefinedAt(x: MysqlConSchemaEntry): Boolean = true
//
//                    override def apply(x: MysqlConSchemaEntry): MysqlConSchemaEntry = if (x.fieldName.toLowerCase == removedColumnName.toLowerCase) x.markAsDeleted(ddlSql) else x
//                  }
//
//                  lazy val nextAcc = acc.collect(markAsDeleted)
//                  loopAlter(tl, nextAcc)
//                }
//              }
//            }
//          }
//
//        }
//
//        lazy val newSchemas = loopAlter(columnModList, lastSchema.schemas)
//          .map(_.updateForNewVersion(newTableName, ddlSql, newVersion, isOriginal, binlogPositionInfo)) //将列更新成最新信息
//        //如果发生了表名变更，触发一次updateTableNameMappingTableIdMap，更新tableId 和tableName的映射关系
//        //        if (!newTableName.equals(oldTableName)) {
//        //          updateTableNameMappingTableIdMap(newTableName, tableId, binlogPositionInfo)
//        //          log.info(s"tableId:${tableId} change tableName from $oldTableName to $newTableName,the info has been updated into TableNameMappingTableIdMap,id:$syncTaskId")
//        //        }
//        MysqlSchemaVersionCollection(
//          dbId,
//          dbName,
//          tableId,
//          newTableName,
//          newVersion,
//          binlogPositionInfo,
//          newSchemas,
//          tableComment,
//          ddlSql = ddlSql,
//          ddlType = ddlType,
//          lastTableId = lastTableId
//        )
//
//      }
//
//      /**
//        * 当发生Create Table语句时
//        * 执行以下操作
//        * 1.更新 tableSchemaVersionMap
//        * 2.更新 tableNameMappingTableIdMap
//        * 3.更新 ddlMap
//        *
//        * checklist:
//        *    1.createTable  √
//        *
//        * @param x
//        * @return
//        */
//      def onCreateTable(x: TableCreate): MysqlSchemaVersionCollection = {
//        log.info(s"ddlSql '$ddlSql' is create ,start onCreateTable,id:$syncTaskId ")
//        lazy val likeTableName: String = Option(x.likeTable).getOrElse("").trim.toLowerCase
//        lazy val isCreateLike: Boolean = likeTableName.nonEmpty
//        lazy val tableName = x.table.toLowerCase
//        lazy val initVersion = 1 // 新建表时，版本默认为1
//        lazy val tableId = tableName + "_" + System.currentTimeMillis()
//        lazy val lastTableId = tableId
//        lazy val ddlType = DdlType.CREATE
//        lazy val isOriginal = true
//        lazy val isDeleted = false
//        lazy val tableComment = ""
//        lazy val primaryKeys = {
//          import scala.collection.JavaConverters._
//          Option(x.pks).map(_.asScala.map(_.toLowerCase))
//        }
//        lazy val columnDefList: List[ColumnDef] = {
//          import scala.collection.JavaConverters._
//          x.columns.asScala.toList
//        }
//        lazy val schemas: List[MysqlConSchemaEntry] = columnDefList.zipWithIndex
//          .map {
//            case (columnDef, hBaseIndex) =>
//              lazy val columnName = columnDef.getName
//              lazy val columnComment = columnDef.getComment
//              lazy val columnDefaultValue = "" //todo 获得默认值
//              SchemaEntry.generateMysqlConSchemaEntry(
//                dbId,
//                dbName,
//                tableId,
//                tableName,
//                hBaseIndex,
//                columnName,
//                columnComment,
//                buildDecimalTypeInfoIfPossible(columnDef),
//                "",
//                initVersion,
//                binlogPositionInfo,
//                isOriginal, // 是初始
//                isDeleted, // 未删除
//                primaryKeys.fold(false)(pks => pks.contains(columnDef.getName.toLowerCase())), //防止pk空指针
//                ddlSql,
//                tableComment,
//                lastTableId, //lastTableId 和 tableId相同
//                ddlType,
//                columnDefaultValue //todo 拿到默认值
//              )
//          }
//        //like语句时专用
//        lazy val schemasOnCreateLike = {
//          log.info("this is an create-like ddl,try to find source tableVersionSchema")
//          Try(getCorrespondingVersionSchema(likeTableName, binlogPositionInfo)) match {
//            case Success(x) => x.schemas
//              .map {
//                schema => schema.copy(tableId = tableId, tableName = tableName, binlogPositionInfo = binlogPositionInfo, ddlSql = ddlSql, ddlType = ddlType, lastTableId = lastTableId, tableComment = tableComment)
//              }
//            case Failure(e) => throw new SourceSchemaNoFoundException(s"cannot find source schema sourceTableName:$likeTableName,tableName:$tableName,id:$syncTaskId,ddlSql:$ddlSql,binlogPosition:$binlogPositionInfo")
//          }
//
//        }
//        lazy val needFlushOtherSink = !tableExists(tableName, binlogPositionInfo)
//        if (!needFlushOtherSink) log.warn(s"cause table:$tableName has already exists,no need to create ,set needFlushOtherSink false,ddlSql:$ddlSql,id:$syncTaskId")
//
//        (isCreateLike) match {
//          case (false) => MysqlSchemaVersionCollection(
//            dbId,
//            dbName,
//            tableId,
//            tableName,
//            initVersion,
//            binlogPositionInfo,
//            schemas,
//            tableComment = tableComment,
//            ddlSql = ddlSql,
//            ddlType = DdlType.CREATE,
//            lastTableId = lastTableId,
//            needFlushOtherSink = needFlushOtherSink
//          )
//          case (true) => MysqlSchemaVersionCollection(
//            dbId,
//            dbName,
//            tableId,
//            tableName,
//            initVersion,
//            binlogPositionInfo,
//            schemasOnCreateLike,
//            tableComment = tableComment,
//            ddlSql = ddlSql,
//            ddlType = DdlType.CREATE,
//            lastTableId = lastTableId,
//            needFlushOtherSink = needFlushOtherSink
//          )
//        }
//
//      }
//
//      /**
//        * 原库表字段不变
//        * 版本加一
//        * HBase 中新建表
//        *
//        * checklist：
//        * truncateTable 未测试
//        *
//        * @param x
//        * @return
//        */
//      def onTruncateTable(x: TableTruncate): MysqlSchemaVersionCollection = {
//        log.info(s"ddlSql '$ddlSql' is truncate ,start onTruncateTable,id:$syncTaskId ")
//        lazy val tableName = x.table.toLowerCase
//        lazy val oldTableId = findTableId(tableName, binlogPositionInfo)
//        lazy val newTableId = tableName + "_truncate_" + System.currentTimeMillis()
//        lazy val lastSchema = getCorrespondingVersionSchema(tableName, binlogPositionInfo)
//        lazy val ddlType = DdlType.TRUNCATE
//        lazy val tableComment = lastSchema.tableComment
//        lazy val newVersion = lastSchema.version + 1
//        lazy val newSchemas = lastSchema.schemas.map(
//          //改为original，这样可以发起一次重新建表
//          _.copy(tableId = newTableId, version = newVersion, isOriginal = true)
//        )
//
//        val schemaVersionCollection = new MysqlSchemaVersionCollection(
//          dbId,
//          dbName,
//          newTableId, // 已更新
//          tableName,
//          newVersion, // 已更新 +1
//          binlogPositionInfo, // 已更新
//          newSchemas, // 已更新
//          tableComment = tableComment,
//          ddlSql = ddlSql,
//          ddlType = ddlType,
//          lastTableId = oldTableId
//        )
//
//        lazy val newTableIds = tableNameMappingTableIdMap.get(tableName)
//          .filter(entry => {
//            entry.tableId != oldTableId
//          })
//          .:+(MysqlTableNameMappingTableIdEntry(newTableId, binlogPositionInfo))
//        //        tableNameMappingTableIdMap.put(tableName, newTableIds)
//        //        ddlMap.put(binlogPositionInfo, ddlSql)
//        schemaVersionCollection
//      }
//
//      /**
//        * drop的逻辑和其他的不太一样
//        * drop是删除
//        * 1.内存 2.mysql元数据 3.hbase 4.hive
//        *
//        * @param x
//        * @return
//        */
//      def onTableDrop(x: TableDrop): MysqlSchemaVersionCollection = {
//        log.info(s"ddlSql '$ddlSql' is drop ,start onTableDrop ,id:$syncTaskId ")
//        lazy val tableName = x.table.toLowerCase
//        //todo 不知道这个是否需要判断表是否存在
//        lazy val isExist = tableExists(tableName, binlogPositionInfo)
//        lazy val notExist = !isExist
//        lazy val needFlushOtherSink = isExist
//        lazy val ddlType = DdlType.DROP
//        lazy val tableId = if (notExist) "" else findTableId(tableName, binlogPositionInfo)
//        lazy val lastSchema = getCorrespondingVersionSchema(tableName, binlogPositionInfo)
//        lazy val lastVersion = if (isExist) lastSchema.version else -1
//        if (notExist) log.warn(s"this table:$dbName.$tableName is not cached in memory,id:$syncTaskId")
//        MysqlSchemaVersionCollection(
//          dbId,
//          dbName,
//          tableId,
//          tableName,
//          lastVersion, //删除特有
//          binlogPositionInfo,
//          null, //用不到
//          "",
//          ddlSql,
//          tableId,
//          ddlType, //返回DROP类型
//          needFlushOtherSink
//        )
//      }
//
//      lazy val listAfterDdlChange: Option[MysqlSchemaVersionCollection] = schemaChange match {
//        case x: TableAlter => Option(onAlterTable(x))
//        case x: TableCreate => Option(onCreateTable(x))
//        case x: TableDrop => Option(onTableDrop(x))
//        case x: TableTruncate => Option(onTruncateTable(x))
//        case _ => log.warn(s"unsupported schemaChange:${schemaChange}"); None
//      }
//
//      listAfterDdlChange
//        .map {
//          columnChange =>
//            log.info(s"new schema after ddl:$ddlSql is $columnChange")
//            columnChange
//        }.flatMap(upsertSchemasInternal(_))
//      //更新回集合中
//
//    }
//  }

  /**
    * 获得匹配的Schema
    * 根据binlogPosition
    * 获得对应的MysqlSchemaVersionCollection
    *
    * 详细的逻辑见内部loopFindVersion
    *
    * @param tbName
    * @param binlogPositionInfo
    * @return
    */
  def getCorrespondingVersionSchema(tbName: String, binlogPositionInfo: BinlogPositionInfo): MysqlSchemaVersionCollection = {
    lazy val tableName = tbName.toLowerCase
    lazy val tableId = findTableId(tableName, binlogPositionInfo)
    //进行一次降序排序，防止顺序乱
    lazy val tableVersionList = tableSchemaVersionMap.get(tableId).toList.map(_._2).sortWith {
      //      (x1, x2) => x1.binlogInfo.timestamp > x2.binlogInfo.timestamp
      //使用binlogPosition排序
      (x1, x2) => BinlogPositionCompare(x1.binlogInfo, x2.binlogInfo)
    }


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
        case hd :: tl => if (BinlogPositionCompare(binlogPositionInfo, hd.binlogInfo)) hd else loopFindVersion(tl)
        case Nil => throw new NoCorrespondingSchemaException(
          {
            lazy val message = s"cannot find MysqlSchemaVersionCollection at tableName:${dbName}.${tableName} when loop Find Version,the list has been run out ,pls check!!!,binlogInfo:$binlogPositionInfo,id:${syncTaskId}"
            log.error(message)
            message
          }
        )
      }
    }

    loopFindVersion(tableVersionList)
  }

  /**
    * 根据BinlogPosition搜索MysqlschemaVersionCollection
    *
    * @param binlogPositionInfo
    * @return
    */
  def searchMysqlSchemaVersionCollection(binlogPositionInfo: BinlogPositionInfo): Option[MysqlSchemaVersionCollection] = {
    import scala.collection.JavaConverters._
    lazy val re = tableSchemaVersionMap
      .asScala
      .map(_._2)
      .flatMap { x => x.values }
      .find(_.binlogInfo.equals(binlogPositionInfo))
    re
  }

  private def tableExists(tableName: String, binlogPositionInfo: BinlogPositionInfo): Boolean = {
    Option(tableNameMappingTableIdMap.get(tableName.toLowerCase)).fold(false)(_ => Try(findTableId(tableName, binlogPositionInfo)).isSuccess)
  }

  /**
    * 检验schema信息是否正确
    *
    * 1.检查tableSchemaVersionMapIds 和 tableNameMappingTableIdMapIds 中的tableId是否一致
    */
  private def schemaVerify: Unit

  = {
    log.info(s"start schema verification,id:$syncTaskId")
    import scala.collection.JavaConverters._
    lazy val tableSchemaVersionMapIds = tableSchemaVersionMap.asScala.map(_._1).toSet
    lazy val tableNameMappingTableIdMapIds = tableNameMappingTableIdMap.asScala.flatMap(_._2.map(_.tableId)).toSet
    lazy val versionDiffMapping = tableSchemaVersionMapIds.diff(tableNameMappingTableIdMapIds)
    lazy val mappingDiffVersion = tableNameMappingTableIdMapIds.diff(tableSchemaVersionMapIds)
    val isSameTableIds = (versionDiffMapping.isEmpty && mappingDiffVersion.isEmpty)

    if (!isSameTableIds) throw new SchemaInconsistentException(s"tableNameMappingTableIdMapIds is diffirent from tableSchemaVersionMapIds, versionDiffMapping:$versionDiffMapping,mappingDiffVersion:$mappingDiffVersion,id:$syncTaskId")
    log.info(s"tableNameMappingTableIdMapIds is same as tableSchemaVersionMapIds,id:$syncTaskId")
    log.info("schema verify success")
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
    * @param versionCollection
    * @return
    */
  private def upsertSchemasInternal(
                                     versionCollection: MysqlSchemaVersionCollection
                                   ): Option[MysqlSchemaVersionCollection]

  = {

    lazy val ddlType = versionCollection.ddlType
    lazy val tableId = versionCollection.tableId
    lazy val newVersion = versionCollection.version
    lazy val binlogPositionInfo = versionCollection.binlogInfo
    lazy val ddlSql = versionCollection.ddlSql
    lazy val tableName = versionCollection.tableName
    lazy val lastTableId = versionCollection.lastTableId

    /**
      * 增加schema信息
      */
    def addSchemaVersion(tableId: String): Unit = {
      if (tableSchemaVersionMap.containsKey(tableId)) {
        lazy val oldSchemas = tableSchemaVersionMap.get(tableId)
        lazy val newSchema = newVersion -> versionCollection
        lazy val newSchemas = oldSchemas.+(newSchema)
        tableSchemaVersionMap.put(tableId, newSchemas)
      } else {
        lazy val newSchema = newVersion -> versionCollection
        tableSchemaVersionMap.put(tableId, Map(newSchema))
      }
      log.info(s"$tableId add new schema into tableSchemaVersionMap,tableId:$syncTaskId")
      //addSchemaVersion时都进行schema信息的增加
      updateTableNameMappingTableIdMap(tableName, tableId, binlogPositionInfo)
    }

    /**
      * 删除schema信息
      */
    def removeSchemaVersion(tableId: String, tableName: Option[String] = None): Unit = {
      tableSchemaVersionMap.remove(tableId)
      tableName
        .flatMap(tbName => Option(tableNameMappingTableIdMap.get(tbName)))
        .foreach {
          oldList =>
            val newList = oldList.filterNot(_.tableId.equals(tableId))
            //todo 此处不好的写法
            if (newList.nonEmpty) tableName.map(tableNameMappingTableIdMap.put(_, newList)) else tableName.map(tableNameMappingTableIdMap.remove(_))
        }

    }

    /**
      * 只有drop回去删除内存中的缓存
      */
    ddlType match {
      case DdlType.DROP => removeSchemaVersion(tableId, Option(tableName))
      case DdlType.TRUNCATE => removeSchemaVersion(lastTableId); addSchemaVersion(tableId)
      case _ => addSchemaVersion(tableId)
    }
    ddlMap.put(binlogPositionInfo, ddlSql)

    Option(versionCollection)
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
  private def updateTableNameMappingTableIdMap(
                                                tableName: String,
                                                tableId: String, binlogPositionInfo: BinlogPositionInfo
                                              )
  = {
    log.info(s"updateTableNameMappingTableIdMap,tableName:$tableName,tableId:$tableId,binlogPositionInfo:$binlogPositionInfo,id:$syncTaskId")
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
    * @param tbName
    * @param binlogPositionInfo
    * @return
    */
  private def findTableId(
                           tbName: String,
                           binlogPositionInfo: BinlogPositionInfo
                         ): String

  = {

    lazy val tableName = tbName.toLowerCase

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
    def loopFindId(remain: List[MysqlTableNameMappingTableIdEntry]): String =
    remain match {
      case hd :: tl => if (BinlogPositionCompare(binlogPositionInfo, hd.binlogInfo)) hd.tableId else loopFindId(tl)
      case Nil => throw new NoCorrespondingTableIdException(
        {
          lazy val message = s"cannot find tableId at tableName when loop Find table Id,the list has been run out ,pls check!!!dbName:${dbName}.${tableName},binlogInfo:$binlogPositionInfo,id:${syncTaskId}"
          log.error(message)
          message
        }
      )
    }

    lazy val list = Option(tableNameMappingTableIdMap.get(tableName))
      .fold(throw new NoCorrespondingTableIdException(s"cannot find tableId at tableName when loop Find table Id,list is null,pls check!!!!!dbName:${dbName}.${tableName},binlogInfo:$binlogPositionInfo,id:${syncTaskId}"))(
        theList => theList.sortWith {
          //进行一次降序排序，防止顺序乱
          //          (x1, x2) => x1.binlogInfo.timestamp > x2.binlogInfo.timestamp
          //使用binlogPosition排序，更稳
          (x1, x2) => BinlogPositionCompare(x1.binlogInfo, x2.binlogInfo)
        }
      )
    loopFindId(list)
  }


  /**
    * 判断binlogPosition 的方法 降序排列
    *
    * @param binlogPositionInfo1
    * @param binlogPositionInfo2
    * @return
    */
  private def BinlogPositionCompare(binlogPositionInfo1: BinlogPositionInfo, binlogPositionInfo2: BinlogPositionInfo): Boolean

  = {
    def buildCompareValue(x: => BinlogPositionInfo): (Long, Long) = {
      lazy val fileNum = x.journalName.split('.')(1).toLong
      lazy val offset = x.offset
      (fileNum, offset)
    }

    lazy val value1 = buildCompareValue(binlogPositionInfo1)
    lazy val value2 = buildCompareValue(binlogPositionInfo2)

    value1._1 - value2._1 match {
      case x if (x > 0) => true
      case x if (x < 0) => false
      case 0 => value1._2 - value2._2 >= 0
    }
  }

//  /**
//    * 如果是精度相关类型的话，进行精度的处理
//    *
//    * @param columnDef
//    * @return
//    */
//  protected def buildDecimalTypeInfoIfPossible(columnDef: => ColumnDef): String = {
//    lazy val lengthInfo = getDecimalTypeLengthIfPossible(columnDef)
//    lazy val theType = columnDef.getType
//    decorateLength(theType, lengthInfo)
//  }

//  /**
//    * 获取精度信息
//    *
//    * @param columnDef
//    * @return
//    */
//  protected def getDecimalTypeLengthIfPossible(columnDef: => ColumnDef): (Option[Int], Option[Int]) = {
//    columnDef match {
//      case col: ColumnDefWithDecimalLength => {
//        lazy val precision: Option[Int] = Try(col.getPrecision.toInt).toOption
//        lazy val scale: Option[Int] = Try(col.getScale.toInt).toOption
//        (precision, scale)
//      }
//      case _ => (None, None)
//    }
//  }

  /**
    * 构造含精度数据的类型是字符串
    *
    * @param t
    * @param columnDecimalLength
    * @return
    */
  protected def decorateLength(t: String, columnDecimalLength: => (Option[Int], Option[Int])): String = {
    columnDecimalLength match {
      case (precision, scale) if scale.nonEmpty => s"$t(${precision.get},${scale.get})"
      case (precision, _) if precision.nonEmpty => s"$t(${precision.get})"
      case _ => t
    }
  }
}

object TableSchemaVersionCache {

  case class MysqlSchemaVersionCollection(
                                           dbId: String,
                                           dbName: String,
                                           tableId: String,
                                           tableName: String,
                                           version: Int,
                                           binlogInfo: BinlogPositionInfo,
                                           schemas: List[MysqlConSchemaEntry],
                                           tableComment: String = "",
                                           ddlSql: String = "",
                                           lastTableId: String = "",
                                           ddlType: DdlType = DdlType.UNKNOWN,
                                           needFlushOtherSink: Boolean = true
                                         ) {
    lazy val fieldSize = schemas.filter(!_.isDeleted).size

    override def toString: String = {
      s"""
         dbId:$dbId,
         dbName:$dbName,
         tableId:$tableId,
         tableName:$tableName,
         version:$version,
         binlogPostionInfo:$binlogInfo
         schemas:${Option(schemas).getOrElse(List.empty).mkString(",")}
         tableComment: $tableComment,
         ddlSql: $ddlSql,
         lastTableId:$lastTableId,
         ddlType: $ddlType,
         needFlushOtherSink:$needFlushOtherSink
       """.stripMargin
    }


  }


  case class MysqlTableNameMappingTableIdEntry(
                                                tableId: String,
                                                binlogInfo: BinlogPositionInfo
                                              )


}


