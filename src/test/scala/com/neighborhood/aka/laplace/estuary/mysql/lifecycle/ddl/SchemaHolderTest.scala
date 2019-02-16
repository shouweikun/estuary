package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.ddl

import com.neighborhood.aka.laplace.estuary.UnitSpec
import com.neighborhood.aka.laplace.estuary.mysql.schema.Parser
import com.neighborhood.aka.laplace.estuary.mysql.schema.tablemeta.MysqlTableSchemaHolder

/**
  * Created by john_liu on 2019/2/16.
  */
final class SchemaHolderTest extends UnitSpec {
  val schemaHolder = new MysqlTableSchemaHolder(Map.empty)
  val databaseName = "test"
  val create1TableName = "table1"
  val create1Sql = s"CREATE TABLE `$create1TableName` ( id int(11) auto_increment not null,PRIMARY KEY (id ))"
  lazy val create1 = Parser.parse(create1Sql, databaseName).head
  val create2TableName = "table2"
  val create2Sql = s"CREATE TABLE if not exists `$create2TableName` ( id int(11) auto_increment not null,PRIMARY KEY (id )) "
  lazy val create2 = Parser.parse(create2Sql, databaseName).head

  val create3TableName = "table2"
  val create3Sql = s"CREATE TABLE if not exists `$create3TableName` ( id int(11) auto_increment not null,`textcol` mediumtext character set 'utf8' not null ,PRIMARY KEY (id )) "
  lazy val create3 = Parser.parse(create3Sql, databaseName).head

  val create4TableName = "table4"
  val create4Sql = s"CREATE TABLE `$create4TableName` LIKE `$databaseName`.`$create1TableName` "
  lazy val create4 = Parser.parse(create4Sql, databaseName).head

  val create5TableName = "table5"
  val create5Sql = s"CREATE TABLE `$create5TableName` LIKE `bar`.`baz` "
  lazy val create5 = Parser.parse(create5Sql, databaseName).head


  "test 1" should "add table schema info into schemaHolder" in {
    schemaHolder.updateTableMeta(create1)
    val tableMetaOption = schemaHolder.getTableMetaByFullName(s"$databaseName.$create1TableName")
    assert(tableMetaOption.isDefined)
    val tableMeta = tableMetaOption.get
    assert(tableMeta.columnInfoMap.size == 1)
    assert(tableMeta.columnInfoMap.contains("id"))
  }

  "test 2" should "add table schema info schemaHodler with create if not exists" in {
    schemaHolder.updateTableMeta(create2)
    val tableMetaOption = schemaHolder.getTableMetaByFullName(s"$databaseName.$create2TableName")
    assert(tableMetaOption.isDefined)
    val tableMeta = tableMetaOption.get
    assert(tableMeta.columnInfoMap.size == 1)
    assert(tableMeta.columnInfoMap.contains("id"))
  }

  "test 3" should "not add table schema info with create if not exists " in {
    schemaHolder.updateTableMeta(create3)
    val tableMetaOption = schemaHolder.getTableMetaByFullName(s"$databaseName.$create3TableName")
    assert(tableMetaOption.isDefined)
    val tableMeta = tableMetaOption.get
    assert(tableMeta.columnInfoMap.size == 1)
    assert(tableMeta.columnInfoMap.contains("id"))
  }
  "test 4" should "add table schema cause like table is existing" in {
    schemaHolder.updateTableMeta(create4)
    val tableMetaOption = schemaHolder.getTableMetaByFullName(s"$databaseName.$create4TableName")
    assert(tableMetaOption.isDefined)
    val tableMeta = tableMetaOption.get
    assert(tableMeta.columnInfoMap.size == 1)
    assert(tableMeta.columnInfoMap.contains("id"))
  }
  "test 5" should "not add table schema cause like table is not existing" in {
    assertThrows[NoSuchElementException](schemaHolder.updateTableMeta(create5))
  }
}
