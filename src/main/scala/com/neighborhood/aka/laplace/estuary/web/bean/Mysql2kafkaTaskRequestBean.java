package com.neighborhood.aka.laplace.estuary.web.bean;

import java.util.Map;

/**
 * Created by john_liu on 2018/3/11.
 */

public class Mysql2kafkaTaskRequestBean extends TaskRequestBean {
    private String syncTaskId;
    private String binlogJournalName;
    private String kafkaPartitionStrategy = "PRIMARY_KEY";
    private long binlogPosition;
    private long binlogTimeStamp = 0;
    private boolean schemaComponentIsOn = false;
    private boolean isDataRemedy = false;
    private boolean isCounting = true;
    private boolean isProfiling = true;
    private boolean isCosting = true;
    private boolean isSavingTaskInfo = false;
    private boolean isPowerAdapted = true;
    private boolean isBlockingFetch = false;
    private int batcherCount = 19;
    private long batchThreshold = 1;
    private long taskStartTime = 1;
    private long fetchDelay = 0;
    private String filterPattern;
    private String filterBlackPattern;
    private String defaultConnectionTimeoutInSeconds;
    private int receiveBufferSize = 16 * 1024 * 1024;
    private int sendBufferSize = 16 * 1024;
    private boolean filterQueryDcl = false;
    private boolean filterQueryDml = false;
    private boolean filterQueryDdl = false;
    private boolean filterRows = false;
    private boolean filterTableError = false;
    private String kafkaBootstrapServers = "";
    private String kafkaBatchSize = "10000";
    private String kafkaMaxBlockMs = "";
    private String kafkaAck = "";
    private String kafkaLingerMs = "";
    private String kafkaRetries = "";
    private String kafkaTopic = "";
    private String kafkaDdlTopic = "";
    private Map<String, String> kafkaSpecficTopics;
    private String mysqladdress;
    private int mysqlPort;
    private String mysqlUsername;
    private String mysqlPassword;
    private String mysqlDefaultDatabase;
    //    private List<String> mysqlDatabases;
    private int listenTimeout = 5000;
    private int listenRetrytime = 3;
    private String concernedDataBase = "";
    private String ignoredDataBase = "";
    private int taskType = 1;
    private long taskEndTime = -1;
    private String hbaseZookeeperQuorum = "";
    private String hbaseZookeeperPropertyClientPort = "";

    // 支持的binlogImage
    // binlog.images = ""
    //支持的binlogFormat
    // binlog.format = ""
    //zookeeper地址,可以设置多个，用";"分隔
    private String zookeeperServers;
    // zookeeper 链接超时设置,单位毫秒
    private int zookeeperTimeout = 10000;


    public String getKafkaBatchSize() {
        return kafkaBatchSize;
    }

    public void setKafkaBatchSize(String kafkaBatchSize) {
        this.kafkaBatchSize = kafkaBatchSize;
    }

    public boolean isSchemaComponentIsOn() {
        return schemaComponentIsOn;
    }

    public void setSchemaComponentIsOn(boolean schemaComponentIsOn) {
        this.schemaComponentIsOn = schemaComponentIsOn;
    }

    public String getKafkaPartitionStrategy() {
        return kafkaPartitionStrategy;
    }

    public void setKafkaPartitionStrategy(String kafkaPartitionStrategy) {
        this.kafkaPartitionStrategy = kafkaPartitionStrategy;
    }

    public boolean isBlockingFetch() {
        return isBlockingFetch;
    }

    public void setBlockingFetch(boolean blockingFetch) {
        isBlockingFetch = blockingFetch;
    }

    public long getTaskStartTime() {
        return taskStartTime;
    }

    public void setTaskStartTime(long taskStartTime) {
        this.taskStartTime = taskStartTime;
    }

    public boolean isSavingTaskInfo() {
        return isSavingTaskInfo;
    }

    public void setSavingTaskInfo(boolean savingTaskInfo) {
        isSavingTaskInfo = savingTaskInfo;
    }

    public boolean isDataRemedy() {
        return isDataRemedy;
    }

    public void setDataRemedy(boolean dataRemedy) {
        isDataRemedy = dataRemedy;
    }

    public String getHbaseZookeeperQuorum() {
        return hbaseZookeeperQuorum;
    }

    public void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
        this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
    }

    public String getHbaseZookeeperPropertyClientPort() {
        return hbaseZookeeperPropertyClientPort;
    }

    public void setHbaseZookeeperPropertyClientPort(String hbaseZookeeperPropertyClientPort) {
        this.hbaseZookeeperPropertyClientPort = hbaseZookeeperPropertyClientPort;
    }


    public int getTaskType() {
        return taskType;
    }

    public void setTaskType(int taskType) {
        this.taskType = taskType;
    }

    public String getIgnoredDataBase() {
        return ignoredDataBase;
    }

    public void setIgnoredDataBase(String ignoredDataBase) {
        this.ignoredDataBase = ignoredDataBase;
    }

    public String getConcernedDataBase() {
        return concernedDataBase;
    }

    public void setConcernedDataBase(String concernedDataBase) {
        this.concernedDataBase = concernedDataBase;
    }

    public String getKafkaDdlTopic() {
        return kafkaDdlTopic;
    }

    public void setKafkaDdlTopic(String kafkaDdlTopic) {
        this.kafkaDdlTopic = kafkaDdlTopic;
    }

    public Map<String, String> getKafkaSpecficTopics() {
        return kafkaSpecficTopics;
    }

    public int getBatcherCount() {
        return batcherCount;
    }

    public void setBatcherCount(int batcherCount) {
        this.batcherCount = batcherCount;
    }

    public void setKafkaSpecficTopics(Map<String, String> kafkaSpecficTopics) {
        this.kafkaSpecficTopics = kafkaSpecficTopics;
    }

    public long getTaskEndTime() {
        return taskEndTime;
    }

    public void setTaskEndTime(long taskEndTime) {
        this.taskEndTime = taskEndTime;
    }

    public long getFetchDelay() {
        return fetchDelay;
    }

    public void setFetchDelay(long fetchDelay) {
        this.fetchDelay = fetchDelay;
    }

    public String getSyncTaskId() {
        return syncTaskId;
    }

    public boolean isCosting() {
        return isCosting;
    }

    public void setCosting(boolean costing) {
        isCosting = costing;
    }

    public void setSyncTaskId(String syncTaskId) {
        this.syncTaskId = syncTaskId;
    }

    public String getBinlogJournalName() {
        return binlogJournalName;
    }

    public void setBinlogJournalName(String binlogJournalName) {
        this.binlogJournalName = binlogJournalName;
    }

    public boolean isPowerAdapted() {
        return isPowerAdapted;
    }

    public void setPowerAdapted(boolean powerAdapted) {
        isPowerAdapted = powerAdapted;
    }

    public long getBinlogPosition() {
        return binlogPosition;
    }

    public void setBinlogPosition(long binlogPosition) {
        this.binlogPosition = binlogPosition;
    }

    public long getBinlogTimeStamp() {
        return binlogTimeStamp;
    }

    public void setBinlogTimeStamp(long binlogTimeStamp) {
        this.binlogTimeStamp = binlogTimeStamp;
    }

    public boolean isCounting() {
        return isCounting;
    }

    public void setCounting(boolean counting) {
        isCounting = counting;
    }

    public boolean isProfiling() {
        return isProfiling;
    }

    public void setProfiling(boolean profiling) {
        isProfiling = profiling;
    }


    public long getBatchThreshold() {
        return batchThreshold;
    }

    public void setBatchThreshold(long batchThreshold) {
        this.batchThreshold = batchThreshold;
    }

    public String getFilterPattern() {
        return filterPattern;
    }

    public void setFilterPattern(String filterPattern) {
        this.filterPattern = filterPattern;
    }

    public String getFilterBlackPattern() {
        return filterBlackPattern;
    }

    public void setFilterBlackPattern(String filterBlackPattern) {
        this.filterBlackPattern = filterBlackPattern;
    }

    public String getDefaultConnectionTimeoutInSeconds() {
        return defaultConnectionTimeoutInSeconds;
    }

    public void setDefaultConnectionTimeoutInSeconds(String defaultConnectionTimeoutInSeconds) {
        this.defaultConnectionTimeoutInSeconds = defaultConnectionTimeoutInSeconds;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public boolean isFilterQueryDcl() {
        return filterQueryDcl;
    }

    public void setFilterQueryDcl(boolean filterQueryDcl) {
        this.filterQueryDcl = filterQueryDcl;
    }

    public boolean isFilterQueryDml() {
        return filterQueryDml;
    }

    public void setFilterQueryDml(boolean filterQueryDml) {
        this.filterQueryDml = filterQueryDml;
    }

    public boolean isFilterQueryDdl() {
        return filterQueryDdl;
    }

    public void setFilterQueryDdl(boolean filterQueryDdl) {
        this.filterQueryDdl = filterQueryDdl;
    }

    public boolean isFilterRows() {
        return filterRows;
    }

    public void setFilterRows(boolean filterRows) {
        this.filterRows = filterRows;
    }

    public boolean isFilterTableError() {
        return filterTableError;
    }

    public void setFilterTableError(boolean filterTableError) {
        this.filterTableError = filterTableError;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaMaxBlockMs() {
        return kafkaMaxBlockMs;
    }

    public void setKafkaMaxBlockMs(String kafkaMaxBlockMs) {
        this.kafkaMaxBlockMs = kafkaMaxBlockMs;
    }

    public String getKafkaAck() {
        return kafkaAck;
    }

    public void setKafkaAck(String kafkaAck) {
        this.kafkaAck = kafkaAck;
    }

    public String getKafkaLingerMs() {
        return kafkaLingerMs;
    }

    public void setKafkaLingerMs(String kafkaLingerMs) {
        this.kafkaLingerMs = kafkaLingerMs;
    }

    public String getKafkaRetries() {
        return kafkaRetries;
    }

    public void setKafkaRetries(String kafkaRetries) {
        this.kafkaRetries = kafkaRetries;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public String getMysqladdress() {
        return mysqladdress;
    }

    public void setMysqladdress(String mysqladdress) {
        this.mysqladdress = mysqladdress;
    }

    public int getMysqlPort() {
        return mysqlPort;
    }

    public void setMysqlPort(int mysqlPort) {
        this.mysqlPort = mysqlPort;
    }

    public String getMysqlUsername() {
        return mysqlUsername;
    }

    public void setMysqlUsername(String mysqlUsername) {
        this.mysqlUsername = mysqlUsername;
    }

    public String getMysqlPassword() {
        return mysqlPassword;
    }

    public void setMysqlPassword(String mysqlPassword) {
        this.mysqlPassword = mysqlPassword;
    }

    public String getMysqlDefaultDatabase() {
        return mysqlDefaultDatabase;
    }

    public void setMysqlDefaultDatabase(String mysqlDefaultDatabase) {
        this.mysqlDefaultDatabase = mysqlDefaultDatabase;
    }

    public int getListenTimeout() {
        return listenTimeout;
    }

    public void setListenTimeout(int listenTimeout) {
        this.listenTimeout = listenTimeout;
    }

    public int getListenRetrytime() {
        return listenRetrytime;
    }

    public void setListenRetrytime(int listenRetrytime) {
        this.listenRetrytime = listenRetrytime;
    }

    public String getZookeeperServers() {
        return zookeeperServers;
    }

    public void setZookeeperServers(String zookeeperServers) {
        this.zookeeperServers = zookeeperServers;
    }

    public int getZookeeperTimeout() {
        return zookeeperTimeout;
    }

    public void setZookeeperTimeout(int zookeeperTimeout) {
        this.zookeeperTimeout = zookeeperTimeout;
    }
}
