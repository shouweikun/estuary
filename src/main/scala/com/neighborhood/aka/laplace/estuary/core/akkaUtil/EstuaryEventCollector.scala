package com.neighborhood.aka.laplace.estuary.core.akkaUtil

import akka.actor.{Actor, ActorLogging}
import com.neighborhood.aka.laplace.estuary.core.eventsource.EstuaryEvent

/**
  * Created by john_liu on 2019/1/14.
  *
  * 针对时间溯源 eventSource使用
  * 收集返回事件
  * 另一方面，方便测试
  *
  * @author neighborhood.aka.laplace
  *
  */
trait EstuaryEventCollector extends Actor with ActorLogging {
  /**
    * 处理event事件
    *
    * @param event 待处理的事件
    * @return 返回的结果
    */
  def handleEvent(event: EstuaryEvent): Any
}
