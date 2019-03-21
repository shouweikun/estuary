package com.neighborhood.aka.laplace.estuary.web.utils

import com.neighborhood.aka.laplace.estuary.bean.credential.{MongoCredentialBean, MysqlCredentialBean}
import com.neighborhood.aka.laplace.estuary.mongo.sink.hbase.HBaseBeanImp
import com.neighborhood.aka.laplace.estuary.mongo.sink.kafka.OplogKeyKafkaBeanImp
import com.neighborhood.aka.laplace.estuary.mongo.source.{MongoOffset, MongoSourceBeanImp}
import com.neighborhood.aka.laplace.estuary.mongo.task.hbase.{Mongo2HBaseAllTaskInfoBean, Mongo2HBaseTaskInfoBeanImp}
import com.neighborhood.aka.laplace.estuary.mongo.task.kafka.{Mongo2KafkaAllTaskInfoBean, Mongo2KafkaTaskInfoBeanImp}
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkBeanImp
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceBeanImp
import com.neighborhood.aka.laplace.estuary.mysql.task.mysql.{Mysql2MysqlAllTaskInfoBean, Mysql2MysqlTaskInfoBeanImp, SdaBean}
import com.neighborhood.aka.laplace.estuary.web.bean._

import scala.collection.JavaConverters._

object TaskBeanTransformUtil {

  def convertMongo2HBaseRequest2Mongo2HBaseTaskInfo(request: Mongo2HBaseTaskRequestBean): Mongo2HBaseAllTaskInfoBean = {
    Mongo2HBaseAllTaskInfoBean(
      sinkBean = HBaseSinkRequestBeanToHBaseBean(request.getHbaseSink),
      sourceBean = mongoSourceRequestBeanToMongoSourceBean(request.getMongoSource),
      taskRunningInfoBean = Mongo2HBaseRunningInfoRequestBeanToMongo2HBaseTaskInfoBean(request.getMongo2HBaseRunningInfo)
    )
  }

  def convertMongo2KafkaRequest2Mongo2KafkaTaskInfo(request: Mongo2KafkaTaskRequestBean): Mongo2KafkaAllTaskInfoBean = {
    val mongoSource = mongoSourceRequestBeanToMongoSourceBean(request.getMongoSource)
    val oplogKeyKafkaSink = OplogKafkaSinkRequestBeanToKafkaSinkBean(request.getKafkaSink)
    val runningInfo = mongo2KafkaRunningInfoRequestBean2Mongo2KafkaTaskInfoBean(request.getMongo2KafkaRunningInfo)
    Mongo2KafkaAllTaskInfoBean(oplogKeyKafkaSink, mongoSource, runningInfo)
  }


  def convertMysql2MysqlRequest2Mysql2MysqlTaskInfo(request: Mysql2MysqlRequestBean): Mysql2MysqlAllTaskInfoBean = {

    val requestMysqlSourceBean = request.getMysqlSourceBean
    val requestMysqlSinkBean = request.getMysqlSinkBean
    val requestRunningBean = request.getMysql2MysqlRunningInfoBean
    val mysqlSourceBean = MysqlSourceBeanImp(
      master = MysqlCredentialRequestBean2MysqlCredentialBean(requestMysqlSourceBean.getMaster),
      concernedDatabase = requestMysqlSourceBean.getConcernedDatabase.asScala.toList,
      ignoredDatabase = Option(requestMysqlSourceBean.getIgnoredDatabase).map(_.asScala.toList).getOrElse(List.empty),
      filterBlackPattern = Option(requestMysqlSourceBean.getFilterBlackPattern),
      filterPattern = Option(requestMysqlSourceBean.getFilterPattern)
    )(
      filterQueryDcl = requestMysqlSourceBean.isFilterQueryDcl,
      filterQueryDml = requestMysqlSourceBean.isFilterQueryDml,
      filterQueryDdl = requestMysqlSourceBean.isFilterQueryDdl,
      filterRows = requestMysqlSourceBean.isFilterRows,
      filterTableError = requestMysqlSourceBean.isFilterTableError
    )
    val mysqlSinkBean = MysqlSinkBeanImp(
      credential = MysqlCredentialRequestBean2MysqlCredentialBean(requestMysqlSinkBean.getCredential)
    )(
      isAutoCommit = Option(requestMysqlSinkBean.isAutoCommit),
      connectionTimeout = if (requestMysqlSinkBean.getConnectionTimeout <= 1l) None else Option(requestMysqlSinkBean.getConnectionTimeout),
      maximumPoolSize = Option(math.max(requestRunningBean.getBatcherNum + 3, requestMysqlSinkBean.getMaximumPoolSize))
    )

    val runningInfoBean = Mysql2MysqlTaskInfoBeanImp(
      syncTaskId = requestRunningBean.getSyncTaskId,
      offsetZkServers = requestRunningBean.getOffsetZkServers
    )(
      isNeedExecuteDDL = requestRunningBean.isNeedExecuteDDL,
      isCosting = requestRunningBean.isCosting,
      isCounting = requestRunningBean.isCounting,
      isProfiling = requestRunningBean.isProfiling,
      isPowerAdapted = requestRunningBean.isPowerAdapted,
      isCheckSinkSchema = requestRunningBean.isCheckSinkSchema,
      schemaComponentIsOn = requestRunningBean.isSchemaComponentIsOn,
      partitionStrategy = requestRunningBean.getPartitionStrategy,
      startPosition = Option(requestRunningBean.getStartPosition),
      batcherNum = requestRunningBean.getBatcherNum,
      batchMappingFormatName = Option(requestRunningBean.getMappingFormatName),
      sinkerNameToLoad = Option(requestRunningBean.getSinkerNameToLoad).map(_.asScala.toMap).getOrElse(Map.empty),
      batcherNameToLoad = Option(requestRunningBean.getBatcherNameToLoad).map(_.asScala.toMap).getOrElse(Map.empty),
      controllerNameToLoad = Option(requestRunningBean.getControllerNameToLoad).map(_.asScala.toMap).getOrElse(Map.empty),
      fetcherNameToLoad = Option(requestRunningBean.getFetcherNameToLoad).map(_.asScala.toMap).getOrElse(Map.empty)
    )
    val sdaBean = Option(request.getSdaBean).map(_.getTableMappingRule.asScala.toMap[String, String]).map(SdaBean(_))
    Mysql2MysqlAllTaskInfoBean(sourceBean = mysqlSourceBean, sinkBean = mysqlSinkBean, taskRunningInfoBean = runningInfoBean, sdaBean = sdaBean)
  }

  private def Mongo2HBaseRunningInfoRequestBeanToMongo2HBaseTaskInfoBean(mongo2HBaseRunningInfoRequestBean: Mongo2HBaseRunningInfoRequestBean): Mongo2HBaseTaskInfoBeanImp = {
    Mongo2HBaseTaskInfoBeanImp(
      syncTaskId = mongo2HBaseRunningInfoRequestBean.getSyncTaskId,
      offsetZookeeperServer = mongo2HBaseRunningInfoRequestBean.getOffsetZookeeperServers
    )(
      mongoOffset = MongoOffset(
        mongoTsSecond = mongo2HBaseRunningInfoRequestBean.getMongoTsSecond,
        mongoTsInc = mongo2HBaseRunningInfoRequestBean.getMongoTsInc
      ),
      batcherNum = if (mongo2HBaseRunningInfoRequestBean.getBatcherNum <= 0) 15 else mongo2HBaseRunningInfoRequestBean.getBatcherNum,
      sinkerNum = if (mongo2HBaseRunningInfoRequestBean.getSinkerNum <= 0) 15 else mongo2HBaseRunningInfoRequestBean.getBatcherNum,
      partitionStrategy = mongo2HBaseRunningInfoRequestBean.getPartitionStrategy,
      batchThreshold = mongo2HBaseRunningInfoRequestBean.getBatchThreshold
    )
  }

  private def HBaseSinkRequestBeanToHBaseBean(hBaseSinkRequestBean: HBaseSinkRequestBean): HBaseBeanImp = {
    HBaseBeanImp(hBaseSinkRequestBean.getHbaseZookeeperQuorum, hBaseSinkRequestBean.getHabseZookeeperPropertyClientPort)
  }

  private def mongoSourceRequestBeanToMongoSourceBean(mongoSourceRequestBean: MongoSourceRequestBean): MongoSourceBeanImp = {
    MongoSourceBeanImp(
      authMechanism = mongoSourceRequestBean.getAuthMechanism,
      mongoCredentials = mongoSourceRequestBean.getMongoCredentials.asScala.map(MongoCredentialRequestBean2MongoCredentialBean(_)).toList,
      hosts = mongoSourceRequestBean.getHosts.asScala.toList,
      port = mongoSourceRequestBean.getPort
    )(
      concernedNs = Option(mongoSourceRequestBean.getConcernedNs).getOrElse(Array.empty),
      ignoredNs = Option(mongoSourceRequestBean.getIgnoredNs).getOrElse(Array.empty)
    )
  }

  private def mongo2KafkaRunningInfoRequestBean2Mongo2KafkaTaskInfoBean(mongo2KafkaRunningInfoRequestBean: Mongo2KafkaRunningInfoRequestBean): Mongo2KafkaTaskInfoBeanImp = {
    Mongo2KafkaTaskInfoBeanImp(
      syncTaskId = mongo2KafkaRunningInfoRequestBean.getSyncTaskId,
      offsetZookeeperServer = mongo2KafkaRunningInfoRequestBean.getOffsetZookeeperServers
    )(
      mongoOffset = MongoOffset(
        mongoTsSecond = mongo2KafkaRunningInfoRequestBean.getMongoTsSecond,
        mongoTsInc = mongo2KafkaRunningInfoRequestBean.getMongoTsInc
      ),
      batcherNum = if (mongo2KafkaRunningInfoRequestBean.getBatcherNum <= 0) 15 else mongo2KafkaRunningInfoRequestBean.getBatcherNum,
      sinkerNum = if (mongo2KafkaRunningInfoRequestBean.getSinkerNum <= 0) 15 else mongo2KafkaRunningInfoRequestBean.getBatcherNum

    )
  }

  private def OplogKafkaSinkRequestBeanToKafkaSinkBean(request: KafkaSinkRequestBean): OplogKeyKafkaBeanImp = {
    OplogKeyKafkaBeanImp(
      bootstrapServers = request.getBootstrapServers,
      topic = request.getTopic,
      ddlTopic = request.getDdlTopic
    )(specificTopics = request.getSpecificTopics.asScala.toMap)
  }

  private def MysqlCredentialRequestBean2MysqlCredentialBean(mysqlCredentialRequestBean: MysqlCredentialRequestBean): MysqlCredentialBean = {
    MysqlCredentialBean(
      address = mysqlCredentialRequestBean.getAddress,
      port = mysqlCredentialRequestBean.getPort,
      username = mysqlCredentialRequestBean.getUsername,
      password = mysqlCredentialRequestBean.getPassword,
      defaultDatabase = Option(mysqlCredentialRequestBean.getDefaultDatabase)
    )
  }

  private def MongoCredentialRequestBean2MongoCredentialBean(mongoCredentialRequestBean: MongoCredentialRequestBean): MongoCredentialBean = {
    MongoCredentialBean(username = mongoCredentialRequestBean.getUsername, database = mongoCredentialRequestBean.getDatabase, password = mongoCredentialRequestBean.getPassword)
  }
}
