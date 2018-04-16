package com.neighborhood.aka.laplace.estuary.web.utils

import com.mongodb.MongoClientURI
import com.typesafe.config.ConfigFactory
import org.mongodb.morphia.{Datastore, Morphia}

class MongoUtils {
  val config = ConfigFactory.load()
  val url = config.getString("akka.mongodb.url")
  val dbName=config.getString("akka.mongodb.dbName")


  //  通过morphia操作mongodb

  def initMongo: Datastore = {
    val morphia = new Morphia
    val mongoClient = new com.mongodb.MongoClient(new MongoClientURI(url))
    val datastore = morphia.createDatastore(mongoClient, dbName)
    morphia.mapPackage("com.neighborhood.aka.laplace.estuary.bean.task.Mysql2kafkaTaskRequestBean")
    datastore.ensureIndexes()
    datastore
  }

}
