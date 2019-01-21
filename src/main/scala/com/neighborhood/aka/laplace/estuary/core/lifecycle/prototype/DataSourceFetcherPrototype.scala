package com.neighborhood.aka.laplace.estuary.core.lifecycle.prototype

import com.neighborhood.aka.laplace.estuary.core.lifecycle.worker.SourceDataFetcher
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import com.neighborhood.aka.laplace.estuary.core.task.SourceManager

/**
  * Created by john_liu on 2018/5/30.
  */
trait DataSourceFetcherPrototype[source <: DataSourceConnection] extends ActorPrototype with SourceDataFetcher {

  /**
    * 数据源资源管理器
    */
  def sourceManager: SourceManager[source]

  /**
    * 数据源链接
    */
  lazy val connection = sourceManager.source  //取消fork，使得线程可以被杀死

  /*
    * 用于在最终的数据汇建立相应的schema信息
    */
  def initEventualSinkSchema: Unit = {}

  /**
    * ********************* Actor生命周期 *******************
    */
  override def preStart(): Unit = {
    log.info(s"fetcher switch to offline,id:$syncTaskId")
    if (connection.isConnected) connection.disconnect()
    //状态置为offline
  }

  override def postRestart(reason: Throwable): Unit = {
    log.info(s"fetcher processing postRestart,id:$syncTaskId")
    super.postRestart(reason)

  }

  override def postStop(): Unit = {
    log.info(s"fetcher processing postStop,id:$syncTaskId")
    connection.disconnect()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.info(s"fetcher processing preRestart,id:$syncTaskId")
    context.become(receive)
    super.preRestart(reason, message)
  }


}
