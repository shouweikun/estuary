package com.neighborhood.aka.laplace.estuary.mongo.source

import com.neighborhood.aka.laplace.estuary.core.offset.ComparableOffset

/**
  * Created by john_liu on 2019/2/27.
  */
final case class MongoOffset(
                              /**
                                * 对应mongodb 中的ts字段中的秒,
                                */
                              mongoTsSecond: Int,

                              /**
                                * 对应mongodb 中的ts字段中的inc, mongoTsSecond与mongoTsInc组成一个Ops log中的唯一值.
                                */
                              mongoTsInc: Int
                            ) extends ComparableOffset[MongoOffset](

) {
  override def compare(other: MongoOffset): Boolean = {
    if (this.mongoTsSecond == other.mongoTsSecond) {
      this.mongoTsInc <= other.mongoTsSecond
    } else this.mongoTsSecond < other.mongoTsSecond

  }

  def formatString = s"($mongoTsSecond,$mongoTsInc)"
}

object MongoOffset {
  def now = MongoOffset((System.currentTimeMillis() / 1000).toInt, 0)
}
