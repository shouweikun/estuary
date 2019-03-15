package com.neighborhood.aka.laplace.estuary.mongo.lifecycle.sink.hbase

import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOffset
import org.apache.hadoop.hbase.client.Put

/**
  * Created by john_liu on 2019/3/14.
  */
private[hbase] case class SinkHolder(
                                      val tableName: String,
                                      val offset: MongoOffset,
                                      val list: java.util.List[Put],
                                      val count:Int,
                                      val generateTs:Long =System.currentTimeMillis()
                                    )