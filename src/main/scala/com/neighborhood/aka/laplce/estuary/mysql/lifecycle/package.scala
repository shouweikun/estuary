package com.neighborhood.aka.laplce.estuary.mysql

/**
  * Created by john_liu on 2018/2/3.
  */
package object lifecycle {
 case class BinlogPositionInfo(journalName:String,offest:Long)
}
