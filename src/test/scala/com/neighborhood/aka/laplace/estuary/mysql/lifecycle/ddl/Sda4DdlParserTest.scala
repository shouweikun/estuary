package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.ddl

import com.neighborhood.aka.laplace.estuary.UnitSpec
import com.neighborhood.aka.laplace.estuary.mysql.schema.defs.ddl._
import com.neighborhood.aka.laplace.estuary.mysql.schema.{Parser, SdaSchemaMappingRule}
import com.neighborhood.aka.laplace.estuary.mysql.schema.Parser.SchemaChangeToDdlSqlSyntax
import scala.collection.JavaConverters._

/**
  * Created by john_liu on 2019/2/14.
  * 目前完成了
  * Alter Table
  * Rename Table
  */
class Sda4DdlParserTest extends UnitSpec {

  val mappingRuleMap = Map("a.a" -> "a_map.a_map", "a.b" -> "a_map.b_map")
  val schemaMappingRule = new SdaSchemaMappingRule(mappingRuleMap)
  val alterTable1 = "ALTER TABLE a.a ADD col1 text DEFAULT 'hello';"
  val alterTable2 = "ALTER TABLE `a`.`a` ADD column `col1` int(11) comment 'c' AFTER `afterCol`"
  val alterTable3 = "ALTER TABLE `a`.`a` ADD column `col1` int(11) unsigned DEFAULT '1' comment 'c' AFTER `afterCol`"
  val alterTable4 = "alter table `a`.`a` add column `c_double` double(10,2) unsigned not null Default '1.00' COMMENT 'c' AFTER `afterCol`"
  val alterTable5 = "alter table `a`.`a` add column `c_decimal` decimal(10,2) unsigned not null Default '1.00' COMMENT 'c' AFTER `afterCol`"
  val alterTable6 = "alter table `a`.`a` drop column `drop`"
  val alterTable7 = "alter table `a`.`a` CHANGE column `foo` bar varchar(255)  default 'foo' not null comment 'c'"
  val alterTable8 = "alter table `a`.`a` MODIFY column bar varchar(255)  default 'foo' not null comment 'c'"
  val alterTable9 = "alter table `a`.`a` CHANGE column `foo` bar decimal(10,2)  default '1.00' not null comment 'c'"
  val alterTable10 = "alter table `a`.`a` MODIFY column bar double(10,2)  default '1.00' not null comment 'c'"
  val renameTable11 = "rename table a.a to a.b"
  val renameTable12 = "rename table a to b"
  val renameTable13 = "rename table a.a to b"
  val renameTable14 = "rename table a to a.b"
  val dropTable15 = "drop table a.a"
  val dropTable16 = "drop table b"
  val dropTable17 = "drop table if exists a.a"
  val createTable18 =
    """create table a.a(
         tutorial_id INT NOT NULL AUTO_INCREMENT,
         tutorial_title VARCHAR(100) NOT NULL,
       tutorial_author VARCHAR(40) NOT NULL,
         submission_date DATE,
         PRIMARY KEY ( tutorial_id )
      );""".stripMargin
  val createTable19 =
    """create table a(
         tutorial_id INT NOT NULL AUTO_INCREMENT,
         tutorial_title VARCHAR(100) NOT NULL,
       tutorial_author VARCHAR(40) NOT NULL,
         submission_date DATE,
         PRIMARY KEY ( tutorial_id )
      );""".stripMargin
  "test 1" should "successfully handle Alter table with column add" in {
    val schemaChange = Parser.parseAndReplace(alterTable1, "a_map", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "a_map")
    val addColumnMod = tableAlter.columnMods.get(0).asInstanceOf[AddColumnMod]
    assert(addColumnMod.definition.getName == "col1")
    assert(addColumnMod.definition.getType == "text")
    assert(addColumnMod.definition.getDefaultValue == "'hello'")
    val ddl = schemaChange.toDdlSql
    assert(ddl == "ALTER TABLE a_map.a_map ADD COLUMN col1 text  DEFAULT 'hello'")
  }

  "test 2" should "successfully handle Alter table with column add" in {
    val schemaChange = Parser.parseAndReplace(alterTable2, "a_map", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "a_map")
    val addColumnMod = tableAlter.columnMods.get(0).asInstanceOf[AddColumnMod]
    assert(addColumnMod.definition.getName == "col1")
    assert(addColumnMod.definition.getType == "int")
    assert(addColumnMod.definition.getDefaultValue == null)
    assert(addColumnMod.definition.getComment == "c")
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "ALTER TABLE a_map.a_map ADD COLUMN col1 int")
  }

  "test 3" should "successfully handle Alter table with column add" in {
    val schemaChange = Parser.parseAndReplace(alterTable3, "a_map", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "a_map")
    val addColumnMod = tableAlter.columnMods.get(0).asInstanceOf[AddColumnMod]
    assert(addColumnMod.definition.getName == "col1")
    assert(addColumnMod.definition.getType == "int")
    assert(addColumnMod.definition.getDefaultValue == "'1'")
    assert(addColumnMod.definition.getComment == "c")
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "ALTER TABLE a_map.a_map ADD COLUMN col1 int unsigned DEFAULT '1'")
  }

  "test 4" should "successfully handle Alter table with column add" in {
    val schemaChange = Parser.parseAndReplace(alterTable4, "a_map", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "a_map")
    val addColumnMod = tableAlter.columnMods.get(0).asInstanceOf[AddColumnMod]
    assert(addColumnMod.definition.getName == "c_double")
    assert(addColumnMod.definition.getType == "double")
    assert(addColumnMod.definition.getFullType == "double(10,2)")
    assert(addColumnMod.definition.getDefaultValue == "'1.00'")
    assert(addColumnMod.definition.getComment == "c")
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "ALTER TABLE a_map.a_map ADD COLUMN c_double double(10,2)  DEFAULT '1.00'")
  }

  "test 5" should "successfully handle Alter table with column add" in {
    val schemaChange = Parser.parseAndReplace(alterTable5, "a_map", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "a_map")
    val addColumnMod = tableAlter.columnMods.get(0).asInstanceOf[AddColumnMod]
    assert(addColumnMod.definition.getName == "c_decimal")
    assert(addColumnMod.definition.getType == "decimal")
    assert(addColumnMod.definition.getFullType == "decimal(10,2)")
    assert(addColumnMod.definition.getDefaultValue == "'1.00'")
    assert(addColumnMod.definition.getComment == "c")
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "ALTER TABLE a_map.a_map ADD COLUMN c_decimal decimal(10,2)  DEFAULT '1.00'")
  }

  "test 6" should "successfully handle table alter with column drop" in {
    val schemaChange = Parser.parseAndReplace(alterTable6, "a_map", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "a_map")
    val removeColumnMod = tableAlter.columnMods.get(0).asInstanceOf[RemoveColumnMod]
    assert(removeColumnMod.name == "drop")
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "ALTER TABLE a_map.a_map DROP COLUMN drop")
  }

  "test 7" should "successfully handle table alter with column change" in {
    val schemaChange = Parser.parseAndReplace(alterTable7, "a_map", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "a_map")
    val changeColumnMod = tableAlter.columnMods.get(0).asInstanceOf[ChangeColumnMod]
    assert(changeColumnMod.name == "foo")
    assert(changeColumnMod.definition.getName == "bar")
    assert(changeColumnMod.definition.getType == "varchar")
    assert(changeColumnMod.definition.getFullType == "varchar(255)")
    assert(changeColumnMod.definition.getDefaultValue == "'foo'")
    assert(changeColumnMod.definition.getComment == "c")
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "ALTER TABLE a_map.a_map CHANGE COLUMN foo  bar varchar(255)  DEFAULT 'foo'")
  }

  "test 8" should "successfully handle table alter with column change" in {
    val schemaChange = Parser.parseAndReplace(alterTable8, "a_map", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "a_map")
    val changeColumnMod = tableAlter.columnMods.get(0).asInstanceOf[ChangeColumnMod]
    assert(changeColumnMod.name == "bar")
    assert(changeColumnMod.definition.getName == "bar")
    assert(changeColumnMod.definition.getType == "varchar")
    assert(changeColumnMod.definition.getFullType == "varchar(255)")
    assert(changeColumnMod.definition.getDefaultValue == "'foo'")
    assert(changeColumnMod.definition.getComment == "c")
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "ALTER TABLE a_map.a_map MODIFY COLUMN   bar varchar(255)  DEFAULT 'foo'")
  }

  "test 9" should "successfully handle table alter with column change" in {
    val schemaChange = Parser.parseAndReplace(alterTable9, "a_map", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "a_map")
    val changeColumnMod = tableAlter.columnMods.get(0).asInstanceOf[ChangeColumnMod]
    assert(changeColumnMod.name == "foo")
    assert(changeColumnMod.definition.getName == "bar")
    assert(changeColumnMod.definition.getType == "decimal")
    assert(changeColumnMod.definition.getFullType == "decimal(10,2)")
    assert(changeColumnMod.definition.getDefaultValue == "'1.00'")
    assert(changeColumnMod.definition.getComment == "c")
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "ALTER TABLE a_map.a_map CHANGE COLUMN foo  bar decimal(10,2)  DEFAULT '1.00'")
  }

  "test 10" should "successfully handle table alter with column change" in {
    val schemaChange = Parser.parseAndReplace(alterTable10, "a_map", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "a_map")
    val changeColumnMod = tableAlter.columnMods.get(0).asInstanceOf[ChangeColumnMod]
    assert(changeColumnMod.name == "bar")
    assert(changeColumnMod.definition.getName == "bar")
    assert(changeColumnMod.definition.getType == "double")
    assert(changeColumnMod.definition.getFullType == "double(10,2)")
    assert(changeColumnMod.definition.getDefaultValue == "'1.00'")
    assert(changeColumnMod.definition.getComment == "c")
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "ALTER TABLE a_map.a_map MODIFY COLUMN   bar double(10,2)  DEFAULT '1.00'")
  }

  "test 11" should "successfully handle table raname with all full name" in {
    val schemaChange = Parser.parseAndReplace(renameTable11, "a_map", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "b_map")
    assert(tableAlter.columnMods.isEmpty)
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "RENAME a_map.a_map TO a_map.b_map;")
  }

  "test 12" should "successfully handle table raname with only table name" in {
    val schemaChange = Parser.parseAndReplace(renameTable12, "a", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "b_map")
    assert(tableAlter.columnMods.isEmpty)
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "RENAME a_map.a_map TO a_map.b_map;")
  }

  "test 13" should "successfully handle table raname with partial table db name" in {
    val schemaChange = Parser.parseAndReplace(renameTable13, "a", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "b_map")
    assert(tableAlter.columnMods.isEmpty)
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "RENAME a_map.a_map TO a_map.b_map;")
  }

  "test 14" should "successfully handle table raname with partial table db name" in {
    val schemaChange = Parser.parseAndReplace(renameTable14, "a", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "b_map")
    assert(tableAlter.columnMods.isEmpty)
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "RENAME a_map.a_map TO a_map.b_map;")
  }

  "test 15" should "successfully handle table drop with full name" in {
    val schemaChange = Parser.parseAndReplace(dropTable15, "a", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableDrop])
    val tableDrop = schemaChange.asInstanceOf[TableDrop]
    assert(tableDrop.database == "a_map")
    assert(tableDrop.table == "a_map")
    assert(!tableDrop.ifExists)
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "DROP TABLE  a_map.a_map")
  }

  "test 16" should "successfully handle table drop only with table name" in {
    val schemaChange = Parser.parseAndReplace(dropTable16, "a", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableDrop])
    val tableDrop = schemaChange.asInstanceOf[TableDrop]
    assert(tableDrop.database == "a_map")
    assert(tableDrop.table == "b_map")
    assert(!tableDrop.ifExists)
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "DROP TABLE  a_map.b_map")
  }

  "test 17" should "successfully handle table drop only with if exists" in {
    val schemaChange = Parser.parseAndReplace(dropTable17, "a", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableDrop])
    val tableDrop = schemaChange.asInstanceOf[TableDrop]
    assert(tableDrop.database == "a_map")
    assert(tableDrop.table == "a_map")
    assert(tableDrop.ifExists)
    val ddl = schemaChange.toDdlSql
    assert(ddl.trim == "DROP TABLE IF EXISTS  a_map.a_map")
  }

  "test 18" should "successfully handle table create with full name" in {
    val schemaChange = Parser.parseAndReplace(createTable18, "a", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableCreate])
    val tableCreate = schemaChange.asInstanceOf[TableCreate]
    assert(tableCreate.database == "a_map")
    assert(tableCreate.table == "a_map")
    assert(tableCreate.ifNotExists == false)
    val cols = tableCreate.columns.asScala.map(_.getName).toSet
    assert(cols.size == 4)
    assert(cols.contains("tutorial_id"))
    assert(cols.contains("tutorial_title"))
    assert(cols.contains("submission_date"))
    assert(cols.contains("tutorial_author"))
    assert(tableCreate.pks.size == 1)
    assert(tableCreate.pks.get(0) == "tutorial_id")
    assert(tableCreate.likeDB == null)
    assert(tableCreate.likeTable == null)
  }


  "test 19" should "successfully handle table create with table name" in {
    val schemaChange = Parser.parseAndReplace(createTable18, "a", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableCreate])
    val tableCreate = schemaChange.asInstanceOf[TableCreate]
    assert(tableCreate.database == "a_map")
    assert(tableCreate.table == "a_map")
    assert(tableCreate.ifNotExists == false)
    val cols = tableCreate.columns.asScala.map(_.getName).toSet
    assert(cols.size == 4)
    assert(cols.contains("tutorial_id"))
    assert(cols.contains("tutorial_title"))
    assert(cols.contains("submission_date"))
    assert(cols.contains("tutorial_author"))
    assert(tableCreate.pks.size == 1)
    assert(tableCreate.pks.get(0) == "tutorial_id")
    assert(tableCreate.likeDB == null)
    assert(tableCreate.likeTable == null)
  }
}
