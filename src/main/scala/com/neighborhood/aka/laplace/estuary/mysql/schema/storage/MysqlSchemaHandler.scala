package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

import java.util.concurrent.ConcurrentHashMap

import com.neighborhood.aka.laplace.estuary.bean.exception.schema._
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.SchemaEntry.MysqlConSchemaEntry
import com.neighborhood.aka.laplace.estuary.mysql.schema.storage.TableSchemaVersionCache.MysqlSchemaVersionCollection
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceManagerImp
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Created by john_liu on 2018/5/31.
  * 数据库表设计 @link http://wiki.puhuitech.cn/pages/viewpage.action?pageId=30059341
  */

class MysqlSchemaHandler(
                          /**
                            * 同步任务Id
                            */
                          val syncTaskId: String,

                          /**
                            * 任务开始时间
                            */
                          val startTimestamp: Long = System.currentTimeMillis(),

                          /**
                            * 任务管理器
                            */
                          val taskManager: MysqlSourceManagerImp with TaskManager,

                          /**
                            * 元数据信息数据库操作处理器
                            */
                          val schemaDataStoreHandler: SchemaDataStoreHandler,

                          /**
                            * sql构造器
                            */
                          val sqlBuilder: SqlBuilder,

                          /**
                            * 针对元数据变更对其他sink影响操作的工具
                            */
                          val otherSinkSchemaFlushUtilOption: Option[OtherSinkSchemaFlushUtil] = None
                        ) {


  /**
    * 日志
    */
  private lazy val log = LoggerFactory.getLogger(s"MysqlSchemaHandler-$syncTaskId")
  /**
    * key dbName
    * value dbId
    */
  lazy val dbNameMappingdbIdMap: ConcurrentHashMap[String, String] = new ConcurrentHashMap[String, String]()
  /**
    * key dbName
    * value TableSchemaVersionCache
    */
  lazy val tableVersionCacheMap: ConcurrentHashMap[String, TableSchemaVersionCache] = new ConcurrentHashMap[String, TableSchemaVersionCache]()

  /**
    * 判断数据库是否被初始化
    * 1.schemaDataStoreHandler 查询数据库
    * 2.根据返回的值进行判断
    * 3 如果成功更新dbNameMappingdbIdMap
    * * @param dbName
    *
    * @return
    */
  def isInitialized(databaseName: => String): Boolean = {
    lazy val dbName = databaseName.toLowerCase
    lazy val list = schemaDataStoreHandler.getInitializedDbId(dbName)
    lazy val size = list.size
    size match {
      case 0 => false
      case 1 => {
        //更新dbName与dbId 映射的关系
        dbNameMappingdbIdMap.put(dbName, list.head);
        true
      }
      case x => throw new DeplicateInitializationException(s"find $x records when check Initialization on dbName:$dbName,id:$syncTaskId")
    }

  }

  /**
    * 主要逻辑在getTableVersionInternal中
    * 最后增加一步校验
    * 如果获取的Schema的字段数量和实际的对不上
    * 返回 None 否则返回正确MysqlSchemaVersionCollection
    *
    * @param databaseName
    * @param tbName
    * @param binlogPositionInfo
    * @return
    */
  def getTableVersion(databaseName: String, tbName: String, binlogPositionInfo: BinlogPositionInfo, fdNameList: List[String]): Option[MysqlSchemaVersionCollection] = {
    lazy val dbName = databaseName.toLowerCase
    lazy val tableName = tbName.toLowerCase
    lazy val fieldNameList = fdNameList.map(_.toLowerCase)
    lazy val tryRe = Try(getTableVersionInternal(dbName, tableName, binlogPositionInfo))
    lazy val re = tryRe.get
    lazy val fieldCount = fieldNameList.size
    lazy val IsSameSize = re.fieldSize == fieldCount
    //将逻辑删除位过滤掉
    lazy val schemaFieldList = re.schemas.withFilter(!_.isDeleted).map(_.fieldName.toLowerCase)
    lazy val getSchemaVersionFailed = tryRe.isFailure
    lazy val nonSameField = fieldNameList.diff(schemaFieldList)
    lazy val IsSameFieldName = nonSameField.size == 0
    if (getSchemaVersionFailed) log.warn(s"cannot get SchemaVersion:$dbName.$tableName-$binlogPositionInfo,should be abnormal,id:$syncTaskId")
    if (getSchemaVersionFailed) return None;
    (IsSameSize, IsSameFieldName) match {
      case (false, _) => {
        log.warn(s"this entry:${binlogPositionInfo} has field(s):{${re.fieldSize} },which not equal to actual field count:$fieldCount,this entry should be abandoned");
        None
      }
      case (_, false) => {
        log.warn(s"this entry:${binlogPositionInfo} has field(s) ${nonSameField.mkString(",")} ,which not equal to actual field(s),this entry should be abandoned");
        None
      }
      case _ => Option(re)
    }

  }

  /**
    * 根据entry 中的ddl sql 更新schema信息
    * 1. 将ddl sql 解析成对应的ddl实体
    * 2. 将ddl 实体装换成SchemaEntryCollection
    * 3. 将实体装换成 SchemaEntryCollection 转换成sql 更新回数据库
    * 4. 将SchemaEntryCollection 添加到 对应的TableSchemaVersionCache中
    * 5. 调用onUpsertSchema
    *
    * 对于已经消费过ddl，尝试创建hbase表和更新hive表
    * 1. 从内存的缓存中搜索对应的SchemaEntryCollection
    * 2. 不更新数据库！！！
    * 3. 调用onUpsertSchema
    *
    * @param ddlSql
    * @return
    */
  def upsertSchema(ddlSql: String, dbName: String, binlogPositionInfo: BinlogPositionInfo): Try[Unit] = {
    lazy val schemaVersionCache = tableVersionCacheMap.get(dbName)
    Option(schemaVersionCache)
      .fold(
        throw new SchemaIsNotInitializedException(s"cannot find dbName :$dbName when get table version，pls check,id:$syncTaskId")
      ) {
        cache =>
          Try(cache.updateSchema(ddlSql, binlogPositionInfo))
          match {
            case Success(x) => log.info(s"$ddlSql put into cache success,id:$syncTaskId"); x
            case Failure(e) => if (e.isInstanceOf[SchemaException] && binlogPositionInfo.timestamp < startTimestamp) None; else throw e
          }
      }
      .fold {
        /**
          * 这里面是已经执行过的ddl
          */
        log.warn(s"ddlSql:$ddlSql,binlogInfo:$binlogPositionInfo is no need to upsert into database，id:$syncTaskId")
        schemaVersionCache
          .searchMysqlSchemaVersionCollection(binlogPositionInfo).fold(throw new SchemaIsNotInitializedException(s"cannot find tableSchemaVersion when searchMysqlSchemaVersionCollection，pls check,id:$syncTaskId"))(onUpdateSchema(_))
      } {
        schemaCollection =>
          schemaCollection.ddlType match {
            case DdlType.UNKNOWN => Try(throw new UnsupportedDdlTypeException(s"DdlType.UNKNOWN is not supported，pls check,id:$syncTaskId"))
            case DdlType.DROP => for {
              _ <- onUpdateSchema(schemaCollection)
              _ <- updateSchemaVersionCollectionIntoDatabase(schemaCollection)
            } yield ()
            case _ => for {
              _ <- updateSchemaVersionCollectionIntoDatabase(schemaCollection)
              _ <- onUpdateSchema(schemaCollection)
            } yield ()
          }

      }
  }

  /**
    * 根据mysql-dbName加载对应的数据库数据缓存
    * 1.构造查询sql
    * 2.数据库查询
    * 3.存入tableVersionCacheMap
    * 4.存入dbNameMappingdbIdMap
    *
    * @param dbName
    */
  def createCache(dbName: String): Unit = {
    lazy val sql = sqlBuilder.getSchemaSql(dbName)
    lazy val schemaVersionCollection = getSchemas(sql, dbName)
    tableVersionCacheMap.put(dbName, schemaVersionCollection)
    // 增加dbName-dbId映射
    dbNameMappingdbIdMap.put(dbName, schemaVersionCollection.dbId)
  }

  /**
    * 根据条件
    * 从tableVersionCacheMap查询
    * 对应的MysqlSchemaVersionCollection
    * 1.根据dbName获得对应的dbId
    * 2.获取dbId对应的tableVersionCache
    * 3.获取【当前条件】下的MysqlSchemaVersionCollection
    *
    * @param dbName
    * @param tableName
    * @param binlogPositionInfo
    * @return MysqlSchemaVersionCollection
    */
  private def getTableVersionInternal(dbName: String, tableName: String, binlogPositionInfo: BinlogPositionInfo): MysqlSchemaVersionCollection = {
    Option(tableVersionCacheMap.get(dbName))
      .fold(throw new SchemaIsNotInitializedException(s"cannot find dbName :$dbName when get table version，pls check,id:$syncTaskId")) {
        map =>
          map.getCorrespondingVersionSchema(tableName, binlogPositionInfo)
      }
  }

  /**
    * 在发生元数据更新时，进行其他操作
    * 1.更新Hive对应表中的元数据信息
    *   1.1如果存在则移除table
    *   1.2建表
    *
    * @param mysqlSchemaVersionCollection
    * @return
    */
  private def onUpdateSchema(mysqlSchemaVersionCollection: => MysqlSchemaVersionCollection): Try[Unit] = {
    lazy val needOtherSinkOperation = mysqlSchemaVersionCollection.needFlushOtherSink
    if (needOtherSinkOperation) otherSinkSchemaFlushUtilOption.map(x => x.flushOtherSink(mysqlSchemaVersionCollection)).getOrElse(Success()) else Try(log.warn(s"no need to flush other sink,id:$syncTaskId"))


  }


  /**
    * 将数据库中schema数据转换为TableSchemaVersionCache对象
    * 1.数据库查询
    * 2.创建并初始化TableSchemaVersionCache
    *
    * @param sql
    * @return
    */
  private def getSchemas(sql: => String, dbName: String): TableSchemaVersionCache = {
    def initTableSchemaVersionCache(fieldList: List[Map[String, Any]]) = {
      lazy val tableSchemaVersionCache = new TableSchemaVersionCache(dbName.toLowerCase, findDbId(dbName).toLowerCase, syncTaskId)
      tableSchemaVersionCache.init(fieldList.map(convertRow2ConSchemaEntry))
      tableSchemaVersionCache
    }

    lazy val fieldList = schemaDataStoreHandler.executeOneSqlQueryByMysqlConnection(sql)
    initTableSchemaVersionCache(fieldList)
  }


  /**
    * 根据dbName寻找DbId
    * 如果第一次没找到就发一次createCache
    * 虽然不是尾递归，但是递归栈深度最多只有2所以没问题
    *
    * @param databaseName
    * @return
    */
  private[estuary] def findDbId(databaseName: String, tried: Boolean = false): String = {
    lazy val dbName = databaseName.toLowerCase
    Option(dbNameMappingdbIdMap.get(dbName))
      .fold {
        if (tried) {
          throw new SchemaIsNotInitializedException(s"cannot find dbId when findDbId at dbName:$dbName,pls check!,id:$syncTaskId")
        }
        else {
          val initialized = (isInitialized(dbName)) //因为isInitialized会去更新dbNameMappingdbIdMap
          if (initialized) createCache(dbName) //再尝试创建一次
          findDbId(dbName, true)
        }
      }(x => x)
  }


  /**
    * 将数据库query的行数据转换为实体
    *
    * @param row
    * @return
    */
  private def convertRow2ConSchemaEntry(
                                         row: Map[String, Any]

                                       ): MysqlConSchemaEntry = {
    //todo
    lazy val effectTimestamp = System.currentTimeMillis()
    lazy val dbName = row("dbName").toString.toLowerCase
    lazy val dbId = row("dbId").toString.toLowerCase
    lazy val tableName = row("tableName").toString.toLowerCase
    lazy val tableId = row("tableId").toString.toLowerCase
    lazy val fieldName = row("fieldName").toString
    lazy val hbaseIndex = row("hbaseIndex").toString.toInt
    lazy val fieldComment = Try(row("fieldComment").toString).getOrElse("")
    lazy val fieldType = row("fieldType").toString
    lazy val trickleFieldType = row("trickleFieldType").toString
    lazy val fieldConstraint = Try(row("fieldConstraint")).getOrElse("unknown").toString
    lazy val timestamp = row("timestamp").toString.toLong * 1000
    lazy val version = row("version").toString.toInt
    lazy val binlogJournalName = row("binlogJournalName").toString
    lazy val binlogPosition = row("binlogPosition").toString.toLong
    lazy val binlogPositionInfo = BinlogPositionInfo(binlogJournalName, binlogPosition, timestamp)
    lazy val isOriginal = if (row("isOriginal").toString.toInt == 1) true else false
    lazy val isDeleted = Try(row("isDeleted").toString.toInt == 1).getOrElse(false)
    lazy val isKey = Try(row("isKey").toString.toLowerCase.trim == "PRI".toLowerCase).getOrElse(false)
    lazy val ddlSql = Try(row("ddlSql").toString).getOrElse("")
    lazy val ddlType = DdlType.getFromString(Try(row("ddlType").toString).getOrElse(""))
    lazy val tableComment = Try(row("tableComment").toString).getOrElse("")
    lazy val lastTableId = Try(row("lastTableId").toString).getOrElse("")
    lazy val defaultValue = Try(row("defaultValue").toString).getOrElse("")
    MysqlConSchemaEntry(
      dbId,
      dbName,
      tableId,
      tableName,
      hbaseIndex,
      fieldName,
      fieldComment,
      fieldType,
      trickleFieldType,
      fieldConstraint,
      version,
      binlogPositionInfo,
      isOriginal,
      isDeleted,
      isKey,
      ddlSql,
      tableComment,
      lastTableId,
      ddlType,
      defaultValue
    )

  }


  private def updateSchemaVersionCollectionIntoDatabase(mysqlSchemaVersionCollection: MysqlSchemaVersionCollection): Try[Unit] = Try {
    /**
      * 删除$tableId 在mysql中对应的元数据信息
      */
    def deleteInfoFromDatabase(mysqlSchemaVersionCollection: MysqlSchemaVersionCollection): Unit = {
      lazy val tableId = mysqlSchemaVersionCollection.tableId
      lazy val dbId = mysqlSchemaVersionCollection.dbId
      schemaDataStoreHandler.deleteInfoFromDatabase(dbId, tableId)
      log.info(s"deleteInfoFromDatabase success,id:$syncTaskId")
    }

    /**
      * 增加$tableId 在mysql中对应的元数据信息
      */
    def addInfoIntoDatabase(mysqlSchemaVersionCollection: MysqlSchemaVersionCollection): Unit = {
      val sqlList = sqlBuilder.transferMysqlSchemaVersionCollection2SqlPrefixParamList(mysqlSchemaVersionCollection)
      schemaDataStoreHandler.updateSqlListIntoDatabase(sqlList) match {
        case false => throw new UpsertSchemaIntoDatabaseFailureException(s"insert sql execution has something wrong,pls check,binlogInfo:${mysqlSchemaVersionCollection.binlogInfo},id:$syncTaskId")
        case true => log.info(s"upsert successfully,$syncTaskId")
      }
    }

    /**
      * 刷新fieldComment
      *
      * @param mysqlSchemaVersionCollection
      * @return
      */
    def flushFieldComment(mysqlSchemaVersionCollection: MysqlSchemaVersionCollection): MysqlSchemaVersionCollection = {
      val schemaName = mysqlSchemaVersionCollection.dbName
      val tableName = mysqlSchemaVersionCollection.tableName
      Try {
        schemaDataStoreHandler.getFieldCommentBySourceMysqlConnection(schemaName.toLowerCase, tableName.toLowerCase)
      }
        .map {
          commentMap =>
            val newSchemaList = mysqlSchemaVersionCollection.schemas
              .map {
                field =>
                  val fieldName = field.fieldName
                  val oldFieldComment = field.fieldComment
                  val newFieldComment = commentMap.get(fieldName).getOrElse(oldFieldComment)
                  field.copy(newFieldComment)
              }

            mysqlSchemaVersionCollection.copy(schemas = newSchemaList)
        }.getOrElse(mysqlSchemaVersionCollection)
    }

    def flushTableComment(mysqlSchemaVersionCollection: MysqlSchemaVersionCollection): MysqlSchemaVersionCollection = {
      val schemaName = mysqlSchemaVersionCollection.dbName
      val tableName = mysqlSchemaVersionCollection.tableName
      Try(schemaDataStoreHandler.getTableCommentBySourceMysqlConnection(schemaName.toLowerCase, tableName.toLowerCase))
        .map {
          comment =>
            if (comment.nonEmpty) mysqlSchemaVersionCollection.copy(tableComment = comment) else mysqlSchemaVersionCollection
        }
        .getOrElse(mysqlSchemaVersionCollection)
    }

    lazy val mysqlSchemaVersionCollectionAfterFlushFieldComment = flushFieldComment(mysqlSchemaVersionCollection)
    lazy val mysqlSchemaVersionCollectionAfterFlushTableComment = flushTableComment(mysqlSchemaVersionCollectionAfterFlushFieldComment)
    lazy val ddlType = mysqlSchemaVersionCollectionAfterFlushFieldComment.ddlType
    lazy val needFlushOtherSink = mysqlSchemaVersionCollectionAfterFlushFieldComment.needFlushOtherSink
    (needFlushOtherSink, ddlType) match {
      case (false, _) => log.warn(s"no need to upsert from database,id:$syncTaskId")
      case (_, DdlType.DROP) => deleteInfoFromDatabase(mysqlSchemaVersionCollection)
      case (_, _) => addInfoIntoDatabase(mysqlSchemaVersionCollectionAfterFlushTableComment)
    }
  }
}

