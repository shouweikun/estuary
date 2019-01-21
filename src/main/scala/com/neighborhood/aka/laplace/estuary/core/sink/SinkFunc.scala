package com.neighborhood.aka.laplace.estuary.core.sink

/**
  * Created by john_liu on 2018/2/7.
  *
  * @note 实现时必须保证线程安全
  * @author neighborhood.aka.laplace
  */
trait SinkFunc {

  /**
    * 创造出一个全新链接
    *
    * @return
    */
  def fork: SinkFunc = ???

  /**
    * 生命周期
    * 关闭
    */
  def close: Unit

  /**
    * 生命周期
    * 开始
    */
  def start: Unit

  /**
    * 检测，是否关闭
    *
    */
  def isTerminated: Boolean
}
