package com.neighborhood.aka.laplace.estuary.mysql.schema.bernie

/**
  * Created by john_liu on 2018/5/30.
  */
sealed trait DDL {

  trait Alter extends DDL
  trait Create extends DDL
  trait Drop extends DDL
  case class AddColumn(db: Option[String], table: String, newColumn: String, dataType: (String, Option[String])) extends Alter

  case class ChangeName(db: Option[String], table: String, oldName: String, newColumn: String, dataType: (String, Option[String])) extends Alter

  case class DropColumn(db: Option[String], table: String, dropColumnName: String) extends

  case class ChangeColumnType(db: Option[String], table: String, column: String, dataType: (String, Option[String])) extends DDL

  case class OtherDDL(sql: String) extends DDL

}
