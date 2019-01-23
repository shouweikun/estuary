package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.ddl

import com.alibaba.otter.canal.parse.inbound.mysql.dbsync.SimpleDdlParser
import com.alibaba.otter.canal.protocol.CanalEntry.EventType
import com.neighborhood.aka.laplace.estuary.UnitSpec

/**
  * Created by john_liu on 2019/1/23.
  */
final class SimpleParserTest extends UnitSpec {

  "test 1" should "parse correctly when it comes to Alter" in {

    val alterDdl = "ALTER TABLE test  DROP xxx"
    val result1 = SimpleDdlParser.parse(alterDdl, "")
    assert(result1.getOriSchemaName == null)
    assert(result1.getOriTableName == null)
    assert(result1.getSchemaName == "")
    assert(result1.getTableName == "test")
    assert(result1.getType == EventType.ALTER)

    val alterDdlReplace = alterDdl.replaceFirst(result1.getTableName, "test1")
    val result2 = SimpleDdlParser.parse(alterDdlReplace, "")
    assert(result2.getOriSchemaName == null)
    assert(result2.getOriTableName == null)
    assert(result2.getSchemaName == "")
    assert(result2.getTableName == "test1")
    assert(result2.getType == EventType.ALTER)
    assert(alterDdlReplace == "ALTER TABLE test1  DROP xxx")
  }

  "test 2" should "parse correctly when it comes to Alter with '`'" in {

    val alterDdl = "alter taBle `test`  DROP xxx"
    val result1 = SimpleDdlParser.parse(alterDdl, "")
    assert(result1.getOriSchemaName == null)
    assert(result1.getOriTableName == null)
    assert(result1.getSchemaName == "")
    assert(result1.getTableName == "test")
    assert(result1.getType == EventType.ALTER)

    val alterDdlReplace = alterDdl.replaceFirst(result1.getTableName, "test1")
    val result2 = SimpleDdlParser.parse(alterDdlReplace, "")
    assert(result2.getOriSchemaName == null)
    assert(result2.getOriTableName == null)
    assert(result2.getSchemaName == "")
    assert(result2.getTableName == "test1")
    assert(result2.getType == EventType.ALTER)
    assert(alterDdlReplace == "alter taBle `test1`  DROP xxx")
  }

  "test 3" should "parse correctly when it comes to Alter with '`'" in {

    val alterDdl = "AlteR Table `test`.`test` Drop xxx"
    val result1 = SimpleDdlParser.parse(alterDdl, "")
    assert(result1.getOriSchemaName == null)
    assert(result1.getOriTableName == null)
    assert(result1.getSchemaName == "test") //which means parameter schemaName is overrided
    assert(result1.getTableName == "test")
    assert(result1.getType == EventType.ALTER)

    val alterDdlReplace = alterDdl.replaceFirst(result1.getSchemaName, "test1")
    val result2 = SimpleDdlParser.parse(alterDdlReplace, "")
    assert(result2.getOriSchemaName == null)
    assert(result2.getOriTableName == null)
    assert(result2.getSchemaName == "test1")
    assert(result2.getTableName == "test")
    assert(result2.getType == EventType.ALTER)
    assert(alterDdlReplace == "AlteR Table `test1`.`test` Drop xxx")
  }

  "" should "" in {
    val pattern = "^\\s+alter\\s+table\\s+(\\w+).*"
     val alterDdl = "alter table `test`.`test` Drop xxx"
    val a =  alterDdl.replace(pattern,"fuck")
    a
  }
}
