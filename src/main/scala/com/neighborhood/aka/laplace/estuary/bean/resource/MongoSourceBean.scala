package com.neighborhood.aka.laplace.estuary.bean.resource

import com.neighborhood.aka.laplace.estuary.bean.credential.MongoCredentialBean
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoOplogConnection

/**
  * Created by john_liu on 2018/4/25.
  *
  * @author neighborhood.aka.laplace
  */
trait MongoSourceBean extends DataSourceBase[MongoOplogConnection] {
  override val dataSourceType = SourceDataType.MONGO.toString  //数据源类型
  val MONGODB_CR = "MONGODB-CR"
  val SCRAM_SHA_1 = "SCRAM-SHA-1"
  /**
    * 读取数据时, 对于有replication set 复本集的collection是使用什么策略
    * primary,
    * primaryPreferred,
    * secondary,
    * secondaryPreferred,
    * nearest
    */
  val readPreference = "secondaryPreferred"
  /**
    * 写数据时的设置, 对于有replication set 复本集的collection是使用什么策略
    * majority,
    * normal,
    * journaled,
    * acknowledged,
    * replica_acknowledged,
    * journal_safe,
    * fsynced,
    * unacknowledged,
    * fsync_safe,
    * safe,
    * replicas_safe,
    * w1,
    * w2,
    * w3
    */
  val writeConcern = "majority"

  def authMechanism:String
  def mongoCredentials: List[MongoCredentialBean]
  def hosts: List[String]
  def port: Int
  def concernedNs: Array[String] = Array.empty
  def ignoredNs: Array[String] = Array.empty

  override def toString: String = "MongoBean{" + ", hosts=" + hosts.mkString(",") + ", port=" + port + ", authMechanism='" + authMechanism + '\'' + ", readPreference='" + readPreference + '\'' + ", writeConcern='" + writeConcern + '\'' + '}'
}
