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

  val alter6TableName = create1TableName
  val alter6Sql = s"Alter table $alter6TableName ADD COLUMN col1 int(11)"
  lazy val alter6 = Parser.parse(alter6Sql, databaseName).head

  val alter7TableName = create1TableName
  val alter7Sql = s"Alter table $alter7TableName CHANGE COLUMN col1 col2 varchar(255)"
  lazy val alter7 = Parser.parse(alter7Sql, databaseName).head

  val alter8TableName = create1TableName
  val alter8Sql = s"Alter table $alter8TableName MODIFY COLUMN  col2 text"
  lazy val alter8 = Parser.parse(alter8Sql, databaseName).head

  val alter9TableName = create1TableName
  val alter9Sql = s"Alter table $alter9TableName DROP COLUMN  col2"
  lazy val alter9 = Parser.parse(alter9Sql, databaseName).head

  val drop10TableName = create1TableName
  val drop10Sql = s"drop table $databaseName.$drop10TableName"
  lazy val drop10 = Parser.parse(drop10Sql, databaseName).head


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
  "test 6" should "update table schema with column add" in {
    schemaHolder.updateTableMeta(alter6)
    val tableMetaOption = schemaHolder.getTableMetaByFullName(s"$databaseName.$alter6TableName")
    assert(tableMetaOption.isDefined)
    val tableMeta = tableMetaOption.get
    assert(tableMeta.columnInfoMap.size == 2)
    assert(tableMeta.columnInfoMap.contains("id"))
    assert(tableMeta.columnInfoMap.contains("col1"))
    assert(tableMeta.columnInfoMap("col1").mysqlType == "int")
  }

  "test 7" should "update table schema with column change" in {
    schemaHolder.updateTableMeta(alter7)
    val tableMetaOption = schemaHolder.getTableMetaByFullName(s"$databaseName.$alter7TableName")
    assert(tableMetaOption.isDefined)
    val tableMeta = tableMetaOption.get
    assert(tableMeta.columnInfoMap.size == 2)
    assert(tableMeta.columnInfoMap.contains("id"))
    assert(tableMeta.columnInfoMap.contains("col2"))
    assert(tableMeta.columnInfoMap("col2").mysqlType == "varchar")
  }

  "test 8" should "update table schema with column modify" in {
    schemaHolder.updateTableMeta(alter8)
    val tableMetaOption = schemaHolder.getTableMetaByFullName(s"$databaseName.$alter8TableName")
    assert(tableMetaOption.isDefined)
    val tableMeta = tableMetaOption.get
    assert(tableMeta.columnInfoMap.size == 2)
    assert(tableMeta.columnInfoMap.contains("id"))
    assert(tableMeta.columnInfoMap.contains("col2"))
    assert(tableMeta.columnInfoMap("col2").mysqlType == "text")
  }

  "test 9" should "update table schema with column drop" in {
    schemaHolder.updateTableMeta(alter9)
    val tableMetaOption = schemaHolder.getTableMetaByFullName(s"$databaseName.$alter9TableName")
    assert(tableMetaOption.isDefined)
    val tableMeta = tableMetaOption.get
    assert(tableMeta.columnInfoMap.size == 1)
    assert(tableMeta.columnInfoMap.contains("id"))
    assert(!tableMeta.columnInfoMap.contains("col2"))
  }

  "test 10" should "drop table schema info with table drop" in {
    schemaHolder.updateTableMeta(drop10)
    val tableMetaOption = schemaHolder.getTableMetaByFullName(s"$databaseName.$drop10TableName")
    assert(tableMetaOption.isEmpty)
  }
}
