package com.neighborhood.aka.laplce.estuary.bean.identity

import java.util.Date

import org.bson.types.ObjectId
import org.mongodb.morphia.annotations.{Id, Version}

/**
  * Created by john_liu on 2018/2/7.
  * 标识唯一任务
  */
trait BaseBean {
  @Id  var id:ObjectId = null

  protected var createTime:Date = null
  protected var lastChange:Date = null
  //@Version 为Entity提供一个乐观锁，动态加载，不需要设置值
  @Version private val version = 0L

  override def toString: String = "BaseBean{" + "id=" + id + ", createTime=" + createTime + ", lastChange=" + lastChange + ", version=" + version + '}'
}
