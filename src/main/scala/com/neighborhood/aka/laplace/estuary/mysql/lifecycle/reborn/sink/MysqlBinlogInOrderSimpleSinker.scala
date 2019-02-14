package com.neighborhood.aka.laplace.estuary.mysql.lifecycle.reborn.sink

import akka.actor.{Actor, ActorLogging}
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import com.neighborhood.aka.laplace.estuary.core.sink.mysql.MysqlSinkFunc

/**
  * Created by john_liu on 2019/1/29.
  */
class MysqlBinlogInOrderSimpleSinker(
                                      sinkFunc: MysqlSinkFunc
                                    ) extends Actor with ActorLogging {
  override def receive: Receive = ???
}
