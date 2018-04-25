package com.neighborhood.aka.laplace.estuary.mongo

import com.mongodb.{BasicDBObject, DBCollection, MongoCredential, ServerAddress}
import com.mongodb.casbah.{MongoClient, MongoClientOptions, ReadPreference}
import com.neighborhood.aka.laplace.estuary.bean.credential.MongoCredentialBean
import com.neighborhood.aka.laplace.estuary.bean.resource.MongoBean
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import org.bson.BsonTimestamp

/**
  * Created by john_liu on 2018/4/23.
  */
class MongoConnection(
                       val mongoBean: MongoBean,
                       val mongoOffset: MongoOffset
                     )
  extends DataSourceConnection {

  private var connector: MongoClient = null
  private var oplogCollection: DBCollection = null
  private var connectFlag = false

  override def connect(): Unit = {
    if (!connectFlag) {
      reconnect()
    }
  }

  override def reconnect(): Unit = {
    connector = buildConnector
    oplogCollection = buildOplogCollection
    connectFlag = true
  }

  override def disconnect(): Unit = {
    if (connectFlag) {
      connector.close()
    }
    connector = null
    oplogCollection = null
    connectFlag = false
  }

  override def isConnected: Boolean = connectFlag

  override def fork: MongoConnection = new MongoConnection(mongoBean, mongoOffset)

  def getConnector = this.connector

  private def buildConnector: MongoClient = {
    lazy val options = createMongoClientOptions
    lazy val hosts = createServerAddress()
    lazy val credential = if (mongoBean.authMechanism.equals(mongoBean.SCRAM_SHA_1)) createScramSha1Credential() else createMongoCRCredential()
    mongoBean
      .mongoCredentials
      .fold(MongoClient(hosts, options))(_ => MongoClient(hosts, credential, options))
  }

  private def prepareQuery(mongoOffset: MongoOffset = this.mongoOffset): BasicDBObject = {
    lazy val ts = ("ts", new BasicDBObject("$gte", new BsonTimestamp(mongoOffset.getMongoTsSecond(), mongoOffset.getMongoTsInc())))
    lazy val op = ("op", new BasicDBObject("$in", Array("i", "u", "d")))
    lazy val fromMigrate = ("fromMigrate", new BasicDBObject("$exists", false))
    //todo 确定是否使用
    // lazy val   ns
    //todo log
    val query = new BasicDBObject()
    List(ts, op, fromMigrate)
      .map(kv => query.put(kv._1, kv._2))
    query
  }

  private def buildOplogCollection = connector.getDB("local").getCollection("oplog.rs")

  private def createMongoClientOptions: com.mongodb.MongoClientOptions = MongoClientOptions.apply(
    readPreference = ReadPreference.Secondary,
    connectionsPerHost = 500,
    connectTimeout = 60000,
    maxWaitTime = 120000,
    socketTimeout = 50000
  )

  private def createServerAddress(hosts: List[String] = mongoBean.hosts, port: Int = mongoBean.port): List[ServerAddress] = {
    hosts.map(new ServerAddress(_, port))
  }

  private def createScramSha1Credential(list: List[MongoCredentialBean] = mongoBean.mongoCredentials.getOrElse(List.empty)): List[MongoCredential] = {
    list
      .withFilter(
        bean =>
          bean.database.isDefined && bean.password.isDefined && bean.username.isDefined
      )
      .map(
        bean =>
          MongoCredential.createScramSha1Credential(bean.username.get, bean.password.get, bean.database.get.toCharArray)
      )
    //.map(MongoCredential.createCredential())
  }

  private def createMongoCRCredential(list: List[MongoCredentialBean] = mongoBean.mongoCredentials.getOrElse(List.empty)): List[MongoCredential] = {
    list
      .withFilter(
        bean =>
          bean.database.isDefined && bean.password.isDefined && bean.username.isDefined
      )
      .map(
        bean =>
          MongoCredential.createMongoCRCredential(bean.username.get, bean.password.get, bean.database.get.toCharArray)
      )
    //.map(MongoCredential.createCredential())
  }
}
