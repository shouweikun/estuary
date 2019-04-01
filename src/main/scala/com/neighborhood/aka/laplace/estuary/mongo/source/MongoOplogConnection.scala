package com.neighborhood.aka.laplace.estuary.mongo.source

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.mongodb.client.MongoCursor
import com.mongodb.{MongoCredential, _}
import com.neighborhood.aka.laplace.estuary.bean.credential.MongoCredentialBean
import com.neighborhood.aka.laplace.estuary.bean.resource.MongoSourceBean
import com.neighborhood.aka.laplace.estuary.core.source.DataSourceConnection
import com.neighborhood.aka.laplace.estuary.mongo.source.MongoConnection._
import org.bson.{BsonTimestamp, Document}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by john_liu on 2019/2/27.
  *
  * @author neighborhood.aka.laplace
  * @note mongo链接 ，用于oplog访问
  */
final class MongoConnection(
                             private val mongoBeanImp: MongoSourceBean
                           ) extends DataSourceConnection {

  private var mongoClient: MongoClient = null
  private val lock = new ReentrantLock()
  private val connectStatus: AtomicBoolean = new AtomicBoolean(false)

  override def connect(): Unit = {
    lock.lock()
    try {
      if (connectStatus.compareAndSet(false, true)) mongoClient = initDbInstance
      connectStatus.set(true)
    } catch {
      case e: Exception => throw e
    }
    finally lock.unlock()
  }

  override def reconnect(): Unit = {
    logger.info("start reconnect mongo connection")
    lock.lock()
    try {
      logger.info("try to close last mongoConnection")
      if (connectStatus.get()) disconnect()
      logger.info("try to connect a new mongoConnection")
      connect()
    } catch {
      case e: Exception => throw e
    }
    finally lock.unlock()
    logger.info("reconnect mongo connection success")
  }

  override def disconnect(): Unit = {
    logger.info("start disconnect mongoConnection")
    lock.lock()
    try {
      if (connectStatus.compareAndSet(true, false)) mongoClient.close()
    } catch {
      case e: Exception => throw e
    }
    finally lock.unlock()
    logger.info("disconnect mongoConnection success")
  }


  override def isConnected: Boolean = {
    connectStatus.get()
  }

  override def fork: MongoConnection = new MongoConnection(mongoBeanImp)

  def getOplogIterator(mongoOffset: MongoOffset = MongoOffset((System.currentTimeMillis() / 1000).toInt, 0)): MongoCursor[Document] = {
    logger.info("start to get oplog iterator")
    lock.lock()
    try {
      if (!connectStatus.get()) connect()
    } catch {
      case e: Exception => throw e
    } finally lock.unlock()
    val iterator = mongoClient
      .getDatabase("local")
      .getCollection("oplog.rs") //oplog表
      .find(prepareOplogQuery(mongoOffset))
      .cursorType(CursorType.TailableAwait) //说明这是一个tail cursor
      //                .addOption(Bytes.QUERYOPTION_SLAVEOK)
      .oplogReplay(true)
      //                    .batchSize(1) //设置batchSize会让tail失效?
      .noCursorTimeout(false) //avoid timeout if you are working on big data
      .maxTime(5, TimeUnit.MINUTES)
      .maxAwaitTime(12, TimeUnit.HOURS) //还是应该有超时时间, 待测试.
      .sort(new BasicDBObject("$natural", 1))
      .iterator()
    iterator
  }

  def findRealDocForUpdate(oplog: Oplog): Option[Document] = {
    assert(this.isConnected, "cannot find real doc cause connection is not ready or disconnected")
    val o = oplog.getCurrentDocument
    if ("u".equals(oplog.getOperateType()) && o != null && o.containsKey("$set")) {
      Try {
        logger.warn(s"try to handle update event for oplog id:${oplog.getId},ts:${oplog.getTimestamp.getTime}${oplog.getTimestamp.getInc},ns:${oplog.getDbName}")
        val o2Iter: MongoCursor[Document] = mongoClient
          .getDatabase(oplog.getDbName())
          .getCollection(oplog.getTableName())
          .find(oplog.getWhereCondition())
          .iterator()
        o2Iter.next()
      }.toOption
    } else None
  }

  /**
    * 初始化mongoClient
    * 构建mongoClient
    *
    * @return mongoClient
    */
  private def initDbInstance: MongoClient

  = {
    logger.info("start init mongo client")
    // mongo3.4.0 读取数据时, 对于有replication set 复本集的collection是使用从库策略
    // http://www.jianshu.com/p/d4c3c9752e7e
    val options = MongoClientOptions.builder.readPreference(ReadPreference.secondaryPreferred).connectionsPerHost(10).connectTimeout(60000).maxWaitTime(120000).socketTimeout(Int.MaxValue).build
    val hosts = mongoBeanImp.hosts.map(host => new ServerAddress(host, mongoBeanImp.port))
    val credential = createCredential(mongoBeanImp.authMechanism, mongoBeanImp.mongoCredentials)
    assert(hosts.nonEmpty) //必须非空
    credential match {
      case Nil => new MongoClient(hosts.asJava, options)
      case _ => new MongoClient(hosts.asJava, credential.asJava, options)
    }

  }

  /**
    * 根据模式构建认证
    *
    * @param credentialMode 认证模式
    * @param list           认证信息
    * @return 构建好的认证模式
    */
  private def createCredential(credentialMode: String, list: List[MongoCredentialBean]): List[MongoCredential]

  = {
    def credentialFunc: (String, String, Array[Char]) => MongoCredential = (name: String, database: String, password: Array[Char]) => credentialMode match {
      case mongoBeanImp.SCRAM_SHA_1 => MongoCredential.createScramSha1Credential(name, database, password)
      case mongoBeanImp.MONGODB_CR => MongoCredential.createMongoCRCredential(name, database, password)
      case _ => throw new RuntimeException("unsupport auth mode when create mongo credential")
    }

    list.map {
      bean => credentialFunc.apply(bean.username, bean.database, bean.password.toCharArray)
    }
  }

  /**
    * 准备oplog的查询条件
    *
    * @param mongoOffset 开始的mongoOffset
    * @return 构建好的查询条件对象
    */
  private def prepareOplogQuery(mongoOffset: MongoOffset): BasicDBObject

  = {
    logger.info("start prepare oplog query")
    val query = new BasicDBObject();
    logger.info(s"query start from ${mongoOffset.mongoTsSecond}:${mongoOffset.mongoTsInc}")
    query.put("ts", new BasicDBObject("$gte", new BsonTimestamp(mongoOffset.mongoTsSecond, mongoOffset.mongoTsInc)))
    query.put("op", new BasicDBObject("$in", List("i", "u", "d", "n").asJava))

    /*
            用来过滤掉因为集群的sharding操作对oplog进行的修改, 参考文档: https://www.mongodb.com/blog/post/tailing-mongodb-oplog-sharded-clusters
         */
    query.put("fromMigrate", new BasicDBObject("$exists", false));
    if (mongoBeanImp.concernedNs.nonEmpty) {
      logger.info(s"table:${mongoBeanImp.concernedNs.mkString(",")} is concerned")
      val concernedTable = mongoBeanImp.concernedNs.toSet.asJava
      query.put("ns", new BasicDBObject("$in", concernedTable))
    }
    if (mongoBeanImp.ignoredNs.nonEmpty) {
      val ignoredTable = mongoBeanImp.ignoredNs.toSet.asJava
      query.put("ns", new BasicDBObject("$nin", ignoredTable))
    }
    //    logger.info(s"mongo query:${query.toJson}")
    query
  }


}

object MongoConnection {
  private[MongoConnection] lazy val logger = LoggerFactory.getLogger(classOf[MongoConnection])
}
