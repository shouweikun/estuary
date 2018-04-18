package com.neighborhood.aka.laplace.estuary.web.dao

import java.util

import com.neighborhood.aka.laplace.estuary.web.bean.TaskRequestBean
import com.neighborhood.aka.laplace.estuary.web.utils.MongoUtils
import org.mongodb.morphia.Datastore
import org.mongodb.morphia.query.Query
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class MongoPersistence[E <: TaskRequestBean] {
  private val LOG = LoggerFactory.getLogger(classOf[MongoPersistence[E]])
  private val mongoUtils: MongoUtils = new MongoUtils
  private val datastore: Datastore = mongoUtils.initMongo


  //  将mysql2kafka的配置信息存入mongodb中
  def save(taskBin: E): E = {
    val saveList = new util.ArrayList[E](1)
    saveList.add(taskBin)
    datastore.save(new java.util.ArrayList[E](saveList))
    taskBin
  }

  //  通过syncTaskId从mongodb中查询
  def getK(clazz: Class[E], syncTaskId: String): E = {
    assert(clazz != null && syncTaskId != null)
    datastore.find(clazz).field("syncTaskId").equal(syncTaskId).get
  }

  //通过自定义key-value查询
  def getKV(clazz: Class[E], key: String, value: String): E = {
    assert(clazz != null)
    var query: Query[E] = datastore.find(clazz)
    query = query.filter(key, value)
    query.get
  }

  //  将key-value以map形式进行过滤查询
  def getBy(clazz: Class[E], filters: util.Map[String, _]): util.List[E] = {
    assert(clazz != null)
    var query: Query[E] = datastore.find(clazz)
    if (filters != null) {
      val keys: util.Set[String] = filters.keySet
      val keyInterator: util.Iterator[String] = keys.iterator
      while ( {
        keyInterator.hasNext
      }) {
        val key: String = keyInterator.next
        query = query.filter(key, filters.get(key))
      }
    }
    query.asList
  }

  // 根据制定条件查询mongdb中是否存在符合条件的document
  def exists(clazz: Class[E], filters: util.Map[String, _]): Boolean = {
    val list: util.List[E] = this.getBy(clazz, filters)
    list != null && list.size > 0
  }

  // 查询所有
  def findAll(clazz: Class[E]): util.List[E] = getBy(clazz, null)



}
object MongoPersistence {
  def apply[E<:TaskRequestBean]: MongoPersistence[E] = new MongoPersistence[E]()
}

