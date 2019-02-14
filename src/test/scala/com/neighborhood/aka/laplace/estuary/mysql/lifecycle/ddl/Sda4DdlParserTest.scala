package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.ddl

import com.neighborhood.aka.laplace.estuary.UnitSpec
import com.neighborhood.aka.laplace.estuary.mysql.schema.defs.ddl.{AddColumnMod, ChangeColumnMod, RemoveColumnMod, TableAlter}
import com.neighborhood.aka.laplace.estuary.mysql.schema.{Parser, SdaSchemaMappingRule}

/**
  * Created by john_liu on 2019/2/14.
  */
class Sda4DdlParserTest extends UnitSpec {

  import com.neighborhood.aka.laplace.estuary.mysql.schema.Parser.SchemaChangeToDdlSqlSyntax

  val mappingRuleMap = Map("a.a" -> "a_map.a_map", "a.b" -> "a_map.b_map")
  val schemaMappingRule = new SdaSchemaMappingRule(mappingRuleMap)
  val alterTable1 = "ALTER TABLE a.a ADD col1 text DEFAULT 'hello';"
  val alterTable2 = "alter table a CHANGE column `foo` bar int(20) unsigned default 1 not null"
  val alterTable3 = "ALTER TABLE a MODIFY foo VARCHAR(200) default 'foo'"
  val alterTable4 = "ALTER TABLE a.a DROP col1"
  val renameTable1 = "rename table a.a TO a.b"

  "test 1" should "successfully handle Alter table add Column" in {
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
    assert(ddl == "ALTER TABLE a_map.a_map ADD col1 text DEFAULT 'hello'")
  }


  "test 2" should "successfully handle Alter Table change column name and type" in {
    val schemaChange = Parser.parseAndReplace(alterTable2, "a", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "a_map")
    val changeColumnMod = tableAlter.columnMods.get(0).asInstanceOf[ChangeColumnMod]
    assert(changeColumnMod.name == "foo")
    assert(changeColumnMod.definition.getName == "bar")
    assert(changeColumnMod.definition.getType == "int")
    assert(changeColumnMod.definition.getDefaultValue == "1")
    val ddl = schemaChange.toDdlSql
    assert(ddl == "ALTER TABLE a_map.a_map CHANGE foo bar int DEFAULT 1")
  }


  "test 3" should "successfully handle Alter Table change column type" in {
    val schemaChange = Parser.parseAndReplace(alterTable3, "a", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "a_map")
    val changeColumnMod = tableAlter.columnMods.get(0).asInstanceOf[ChangeColumnMod]
    assert(changeColumnMod.name == "foo")
    assert(changeColumnMod.definition.getName == "foo")
    assert(changeColumnMod.definition.getType == "varchar")
    assert(changeColumnMod.definition.getDefaultValue == "'foo'")
    val ddl = schemaChange.toDdlSql
    assert(ddl == "ALTER TABLE a_map.a_map CHANGE foo foo varchar DEFAULT 'foo'")
  }


  "test 4" should "successfully handle Alter Table drop column" in {
    val schemaChange = Parser.parseAndReplace(alterTable4, "a_map", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "a_map")
    val removeColumnMod = tableAlter.columnMods.get(0).asInstanceOf[RemoveColumnMod]
    assert(removeColumnMod.name == "col1")
    val ddl = schemaChange.toDdlSql
    assert(ddl == "ALTER TABLE a_map.a_map DROP col1")
  }

  "test 5" should "successfully handle rename Table" in {
    val schemaChange = Parser.parseAndReplace(renameTable1, "a_map", schemaMappingRule)
    assert(schemaChange.isInstanceOf[TableAlter])
    val tableAlter = schemaChange.asInstanceOf[TableAlter]
    assert(tableAlter.database == "a_map")
    assert(tableAlter.table == "a_map")
    assert(tableAlter.newDatabase == "a_map")
    assert(tableAlter.newTableName == "b_map")
    assert(tableAlter.columnMods.size() == 0)
    val ddl = schemaChange.toDdlSql
    assert(ddl == "RENAME a_map.a_map TO a_map.b_map;")

  }

}
