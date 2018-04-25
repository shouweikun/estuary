package com.neighborhood.aka.laplace.estuary.mongo

import com.mongodb.{MongoCredential, ServerAddress}
import com.mongodb.casbah.{MongoClient, MongoClientOptions, ReadPreference}
import com.neighborhood.aka.laplace.estuary.bean.credential.MongoCredentialBean
import com.neighborhood.aka.laplace.estuary.bean.resource.MongoBean
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection

/**
  * Created by john_liu on 2018/4/23.
  */
class MongoConnection(
                       val mongoBean: MongoBean
                     )
  extends DataSourceConnection {

  private lazy val connector = buildConnector
  private lazy val oplogCollection = null
  override def connect(): Unit = {

  }

  override def reconnect(): Unit = {}

  override def disconnect(): Unit = {}

  override def isConnected: Boolean = ???

  override def fork: MongoConnection = ???

  def buildConnector: MongoClient = {
    lazy val options = createMongoClientOptions
    lazy val hosts = createServerAddress()
    lazy val credential = if (mongoBean.authMechanism.equals(mongoBean.SCRAM_SHA_1)) createScramSha1Credential() else createMongoCRCredential()
    mongoBean
      .mongoCredentials
      .fold(MongoClient(hosts, options))(_ => MongoClient(hosts, credential, options)
      )
  }

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
