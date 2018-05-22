package com.neighborhood.aka.laplace.estuary.bean.identity

import java.util.Date

import org.bson.types.ObjectId
import org.mongodb.morphia.annotations.{Id, Version}

/**
  * Created by john_liu on 2018/2/7.
  * 标识唯一任务
  */
trait BaseBean {
   var id:ObjectId = null

  protected var createTime:Date = null
  protected var lastChange:Date = null

   private val version = 0L

  override def toString: String = "BaseBean{" + "id=" + id + ", createTime=" + createTime + ", lastChange=" + lastChange + ", version=" + version + '}'
}
