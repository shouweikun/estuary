package com.neighborhood.aka.laplace.estuary.core.task

import com.neighborhood.aka.laplace.estuary.bean.resource.DataSourceBase
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection

import scala.util.Try

/**
  * Created by john_liu on 2019/1/13.
  */
trait SourceManager[S <: DataSourceConnection] {
  /**
    * 数据源bean
    */
  def sourceBean: DataSourceBase[S]

  /**
    * 数据源
    */
  def source: S = source_

  private lazy val source_ : S = buildSource

  /**
    * 构建数据源
    *
    * @return source
    */
  def buildSource: S

  /**
    * 停止所有资源
    */
  def closeSource:Unit = Try{if(source.isConnected)source.disconnect()}
}
