package com.neighborhood.aka.laplace.estuary.core.schema

/**
  * Created by john_liu on 2018/6/1.
  */

trait EventualSinkSchemaHandler[A] {


  /**
    * 创建db
    */
  def createDb(info: A):Unit

  /**
    * 删除db
    */
  def dropDb(info:A):Unit
  /**
    * 创建表
    */
  def createTable(info:A):Unit

  /**
    * 删除表
    */
  def dropTable(info:A):Unit

  /**
    * 查看库表是否存在
    */

  def isExists(info:A):Boolean

  /**
    * 如果不存在就创建
    */
  def createTableIfNotExists(info:A):Unit

  def dropTableIfExists(info:A):Unit

}
