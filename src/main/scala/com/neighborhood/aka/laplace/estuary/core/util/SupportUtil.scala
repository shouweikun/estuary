package com.neighborhood.aka.laplace.estuary.core.util

import com.neighborhood.aka.laplace.estuary.bean.exception.other.TimeoutException
import com.neighborhood.aka.laplace.estuary.core.task.TaskManager
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

/**
  * Created by john_liu on 2018/9/3.
  */
object SupportUtil {
  private lazy val logger = LoggerFactory.getLogger(SupportUtil.getClass)

  /**
    * 判断当前数据是否发送完毕(fetch == sink)
    *
    * @param taskManager 任务管理器
    * @return
    */
  def sendCurrentAllDataFinish(taskManager: TaskManager): Boolean = {
    lazy val fetchCount = taskManager.fetchCount.get()
    lazy val sinkCount = taskManager.batchCount.get()
    lazy val sameCount = fetchCount == sinkCount
    sameCount
  }

  /**
    * 循环判断当前数据是否发送完毕
    *
    * @param taskManager 任务管理器
    * @param timeout     超时时间，单位是ms
    * @param startTs     开始的时间戳
    * @throws TimeoutException
    */
  @tailrec
  @throws[TimeoutException]
  def loopWaiting4SendCurrentAllDataFinish(taskManager: TaskManager, timeout: Option[Long] = None, startTs: Long = System.currentTimeMillis()): Unit = {
    lazy val currentTs = System.currentTimeMillis()
    lazy val totalCost = currentTs - startTs
    lazy val isTimeout = timeout.fold(false)(t => totalCost >= t)
    if (isTimeout) {
      logger.warn(s"time has been run out when loopWaiting4SendDataFinish,currentTs:$currentTs,timeOut:$timeout,startTs:$startTs")
      throw new TimeoutException(s"time has been run out when loopWaiting4SendDataFinish,currentTs:$currentTs,timeOut:$timeout,startTs:$startTs")
    } else loopWaiting4SendCurrentAllDataFinish(taskManager, timeout, startTs)
  }
}
