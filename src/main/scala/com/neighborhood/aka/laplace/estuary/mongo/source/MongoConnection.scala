package com.neighborhood.aka.laplace.estuary.mongo.source

import com.mongodb._
import com.mongodb.casbah.{MongoClient, MongoClientOptions, ReadPreference}
import com.mongodb.client.model.DBCollectionFindOptions
import com.neighborhood.aka.laplace.estuary.bean.credential.MongoCredentialBean
import com.neighborhood.aka.laplace.estuary.bean.resource.MongoBean
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import org.bson.BsonTimestamp
import org.slf4j.LoggerFactory

/**
  * Created by john_liu on 2018/4/23.
  *
  * @todo query 时间开始节点，无限getNext？
  *
  */
class MongoConnection(
                       val mongoBean: MongoBean //mongo信息
                     )
  extends DataSourceConnection {

  private val logger = LoggerFactory.getLogger(classOf[MongoConnection])
  private var connector: MongoClient = null
  private var oplogCollection: DBCollection = null
  private var connectFlag = false

  override def connect(): Unit = {
    if (!connectFlag) {
      reconnect()
    } else {
      logger.warn(s"mongoClient:${mongoBean} cannot be connected twice")
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
      logger.debug(s"mongoClient:${mongoBean} has been disconnected")
    }
    connector = null
    oplogCollection = null
    connectFlag = false
  }

  override def isConnected: Boolean = connectFlag

  override def fork: MongoConnection = new MongoConnection(mongoBean)

  def getConnector = this.connector

  def QueryOplog(mongoOffset: MongoOffset): DBCursor = {
    if (!isConnected) connect()
    lazy val findOptions = {
      new DBCollectionFindOptions()
        .cursorType(CursorType.TailableAwait)
        .oplogReplay(true)
        .noCursorTimeout(true)
        .sort((new BasicDBObject("$natural", 1))).limit(if (mongoOffset.getMongoLimit() > 0) mongoOffset.getMongoLimit else Int.MaxValue)
    }
    buildOplogCollection
      .find(prepareQuery(mongoOffset), findOptions)

  }

  /**
    * 创建Connector
    *
    * @return MongoClient
    */
  private def buildConnector: MongoClient = {
    lazy val options = createMongoClientOptions
    lazy val hosts = createServerAddress()
    lazy val credential = if (mongoBean.authMechanism.equals(mongoBean.SCRAM_SHA_1)) createScramSha1Credential() else createMongoCRCredential()
    mongoBean
      .mongoCredentials
      .fold(MongoClient(hosts, options))(_ => MongoClient(hosts, credential, options))
  }

  /**
    *
    * 根据初始信息构建mongoQuery
    *
    * @param startMongoOffset 开始的mongoOffset
    * @return
    */
  private def prepareQuery(startMongoOffset: MongoOffset): BasicDBObject = {
    lazy val ts = ("ts", new BasicDBObject("$gte", new BsonTimestamp(startMongoOffset.getMongoTsSecond(), startMongoOffset.getMongoTsInc())))
    lazy val op = ("op", new BasicDBObject("$in", Array("i", "u", "d")))
    lazy val fromMigrate = ("fromMigrate", new BasicDBObject("$exists", false))
    //todo log
    lazy val ns = null

    val query = new BasicDBObject()
    List(ts, op, fromMigrate)
      .map(kv => query.put(kv._1, kv._2))
    query
  }

  /**
    * oplog的collection集合
    *
    * @return
    */
  private def buildOplogCollection = connector.getDB("local").getCollection("oplog.rs")

  /**
    * Mongo链接的设置
    *
    * @return
    */
  private def createMongoClientOptions: com.mongodb.MongoClientOptions = MongoClientOptions.apply(
    readPreference = ReadPreference.Secondary,
    connectionsPerHost = 500,
    connectTimeout = 60000,
    maxWaitTime = 120000,
    socketTimeout = 50000
  )

  /**
    * 创建server address
    *
    * @param hosts
    * @param port
    * @return
    */
  private def createServerAddress(hosts: List[String] = mongoBean.hosts, port: Int = mongoBean.port): List[ServerAddress] = {
    hosts.map(new ServerAddress(_, port))
  }

  /**
    * 创建加密Sha1验证
    *
    * @param list
    * @return
    */
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

  /**
    * 创建CRC加密校验
    *
    * @param list
    * @return
    */
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
