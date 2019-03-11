package com.neighborhood.aka.laplace.estuary.web.utils

import com.neighborhood.aka.laplace.estuary.bean.credential.MysqlCredentialBean
import com.neighborhood.aka.laplace.estuary.mysql.sink.MysqlSinkBeanImp
import com.neighborhood.aka.laplace.estuary.mysql.source.MysqlSourceBeanImp
import com.neighborhood.aka.laplace.estuary.mysql.task.mysql.{Mysql2MysqlAllTaskInfoBean, Mysql2MysqlTaskInfoBeanImp, SdaBean}
import com.neighborhood.aka.laplace.estuary.web.bean.{Mysql2MysqlRequestBean, MysqlCredentialRequestBean}

object TaskBeanTransformUtil {


  def convertMysql2MysqlRequest2Mysql2MysqlTaskInfo(request: Mysql2MysqlRequestBean): Mysql2MysqlAllTaskInfoBean = {
    import scala.collection.JavaConverters._
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

    val tableMappingRule = Option(request.getSdaBean).map(_.getTableMappingRule.asScala.toMap[String, String]).getOrElse(Map.empty)
    val encryptionField = Option(request.getSdaBean).map(_.getEncryptField.asScala.mapValues(_.asScala.toSet).toMap[String, Set[String]]).getOrElse(Map.empty)
    val sdaBean = Option(SdaBean(tableMappingRule, encryptionField))
    Mysql2MysqlAllTaskInfoBean(sourceBean = mysqlSourceBean, sinkBean = mysqlSinkBean, taskRunningInfoBean = runningInfoBean, sdaBean = sdaBean)
  }


  def MysqlCredentialRequestBean2MysqlCredentialBean(mysqlCredentialRequestBean: MysqlCredentialRequestBean): MysqlCredentialBean = {
    MysqlCredentialBean(
      address = mysqlCredentialRequestBean.getAddress,
      port = mysqlCredentialRequestBean.getPort,
      username = mysqlCredentialRequestBean.getUsername,
      password = mysqlCredentialRequestBean.getPassword,
      defaultDatabase = Option(mysqlCredentialRequestBean.getDefaultDatabase)
    )
  }
}
