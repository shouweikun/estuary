package com.neighborhood.aka.laplace.estuary.mysql.schema.storage

/**
  * Created by john_liu on 2018/7/6.
  */
object DdlType extends Enumeration {
  type DdlType = Value
  val CREATE = Value(0)
  val AlTER = Value(1)
  val DROP = Value(2)
  val TRUNCATE = Value(3)
  val UNKNOWN = Value(4)

  def ddlTypetoString(ddlType: DdlType): String = {
    ddlType match {
      case CREATE => "create"
      case AlTER => "alter"
      case DROP => "drop"
      case TRUNCATE => "truncate"
      case UNKNOWN => "unknown"
    }
  }

  def getFromString(s: String): DdlType = {
    s.trim.toLowerCase match {
      case "create" => CREATE
      case "alter" => AlTER
      case "drop" => DROP
      case "truncate" => TRUNCATE
      case _ => UNKNOWN
    }
  }
}
