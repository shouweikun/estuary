package com.neighborhood.aka.laplace.estuary.mysql.schema.tablemeta

import com.neighborhood.aka.laplace.estuary.core.util.JavaCommonUtil
import com.neighborhood.aka.laplace.estuary.mysql.schema.defs.ddl._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try


/**
  * Created by john_liu on 2019/1/23.
  *
  * mysql tables的元数据信息
  *
  * @author neighborhood.aka.laplace
  */
final class MysqlTableSchemaHolder(
                                    @volatile private var tableSchemas: Map[String, EstuaryMysqlTableMeta]
                                  ) {

  /**
    * 获取tableMeta
    *
    * @param fullName $databaseName.$tableName
    * @return 找到Some(x) else None
    */
  def getTableMetaByFullName(fullName: String): Option[EstuaryMysqlTableMeta] = tableSchemas.get(fullName)

  /**
    * 更新tableMeta信息
    *
    * @param schemaChange
    */
  def updateTableMeta(schemaChange: SchemaChange): Unit = {
    schemaChange match {
      case tableAlter: TableAlter => handleTableAlter(tableAlter)
      case tableCreate: TableCreate => handleTableCreate(tableCreate)
      case tableTruncate: TableTruncate => //do nothing
      case tableDrop: TableDrop => handleTableDrop(tableDrop)
      case _ => //do nothing
    }
  }

  /**
    * 处理tableDrop
    *
    * @param drop
    */
  private def handleTableDrop(drop: TableDrop): Unit = {
    tableSchemas = tableSchemas.filterNot(x => x._1 == s"${drop.database}.${drop.table}")
  }

  /**
    * 处理创建表
    * 支持like 语句
    * 如果Schema 缓存中存在 key ，则不更新
    *
    * @param create
    */
  private def handleTableCreate(create: TableCreate): Unit = {
    val tableName = create.table
    val dbName = create.database
    val key = s"$dbName.$tableName"
    lazy val tableSchemaFromLikeTable = tableSchemas(s"$dbName.${create.likeTable}").copy(tableName = tableName)
    if (!tableSchemas.contains(key)) {
      if (!JavaCommonUtil.isEmpty(create.likeTable)) tableSchemas = tableSchemas + (key -> tableSchemaFromLikeTable)
      else {
        val columnInfoList = create.columns.asScala.map(_.toEstuaryMysqlColumnInfo).toList
        tableSchemas = tableSchemas.updated(key, EstuaryMysqlTableMeta(dbName, tableName, columnInfoList))
      }
    }
  }

  /**
    * 处理Alter/Rename
    *
    * @param alter TableAlter
    */
  private def handleTableAlter(alter: TableAlter): Unit = {
    val newDatabaseName = Option(alter.newDatabase).getOrElse(alter.database)
    val newTableName = Option(alter.newTableName).getOrElse(alter.table)
    val key = s"${newDatabaseName}.${newTableName}"
    val oldColumns = tableSchemas(key).columns
    val mods = Try(alter.columnMods.asScala.toList).getOrElse(List.empty)

    @tailrec
    def loopBuild(mods: List[ColumnMod] = mods, acc: List[EstuaryMysqlColumnInfo] = oldColumns): List[EstuaryMysqlColumnInfo] = {
      mods match {
        case hd :: tl => hd match {
          case add: AddColumnMod => loopBuild(tl, add.definition.toEstuaryMysqlColumnInfo :: acc)
          case remove: RemoveColumnMod => loopBuild(tl, acc.filter(x => x.name == remove.name))
          case change: ChangeColumnMod => loopBuild(
            tl, acc.map { column => if (column.name == change.definition.getName) change.definition.toEstuaryMysqlColumnInfo else column })
        }
        case Nil => acc
      }

    }

    tableSchemas = tableSchemas.updated(key, EstuaryMysqlTableMeta(alter.newDatabase, alter.newTableName, loopBuild()))
  }

}
