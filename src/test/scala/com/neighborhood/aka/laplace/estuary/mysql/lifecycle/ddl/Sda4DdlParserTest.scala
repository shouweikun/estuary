package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.ddl

import com.neighborhood.aka.laplace.estuary.UnitSpec
import com.neighborhood.aka.laplace.estuary.mysql.schema.defs.ddl.{AddColumnMod, ChangeColumnMod, TableAlter}
import com.neighborhood.aka.laplace.estuary.mysql.schema.{Parser, SdaSchemaMappingRule}

/**
  * Created by john_liu on 2019/2/14.
  */
class Sda4DdlParserTest extends UnitSpec {

  val mappingRuleMap = Map("a.a" -> "a_map.a_map", "b.b" -> "b_map.b_map")
  val schemaMappingRule = new SdaSchemaMappingRule(mappingRuleMap)
  val alterTable1 = "ALTER TABLE a.a ADD col1 text DEFAULT 'hello';"
  val alterTable2 = "alter table a CHANGE column `foo` bar int(20) unsigned default 1 not null"
  val alterTable3 = "ALTER TABLE a MODIFY foo VARCHAR(200) default 'foo'"
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
  }
}
