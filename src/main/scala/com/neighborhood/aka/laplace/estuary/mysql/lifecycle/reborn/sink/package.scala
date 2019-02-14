package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn

import com.neighborhood.aka.laplace.estuary.mysql.lifecycle.BinlogPositionInfo

/**
  * Created by john_liu on 2019/1/30.
  */
package object sink {

  private[sink] final case class SqlList(list: List[String], binlogPositionInfo: Option[BinlogPositionInfo], shouldCount: Int, ts: Long = System.currentTimeMillis())

}
