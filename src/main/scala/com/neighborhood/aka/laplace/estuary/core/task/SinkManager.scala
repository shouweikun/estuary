package com.neighborhood.aka.laplace.estuary.core.task

import com.neighborhood.aka.laplace.estuary.bean.datasink.DataSinkBean
import com.neighborhood.aka.laplace.estuary.core.sink.SinkFunc
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by john_liu on 2019/1/13.
  */
trait SinkManager[S <: SinkFunc] {

  protected lazy val logger = LoggerFactory.getLogger(classOf[SinkManager[S]])


  def syncTaskId:String
  /**
    * 数据汇bean
    */
  def sinkBean: DataSinkBean[S]


  /**
    * 数据汇
    */
  def sink: S = sink_

  private lazy val sink_ : S = buildSink

  /**
    * 构建数据汇
    *
    * @return sink
    */
  def buildSink: S

  /**
    * 统一关闭资源
    */
  def closeSink: Unit = Try {
    logger.info(s"processing close sink,id:$syncTaskId")
    if (!sink.isTerminated) sink.close
  }

  /**
    * 统一开启Sink
    */
  def startSink: Unit = {
    logger.info(s"processing start sink,id:$syncTaskId")
    sink.start
  }


}
