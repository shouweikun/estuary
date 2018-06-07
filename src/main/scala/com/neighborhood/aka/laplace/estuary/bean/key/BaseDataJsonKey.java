package com.neighborhood.aka.laplace.estuary.bean.key;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Created by z on 17-3-31.
 * toDo
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BaseDataJsonKey implements Cloneable {

    /**
     * 分区策略
     */
    public PartitionStrategy partitionStrategy = PartitionStrategy.MOD;
    /**
     * Application Server的名称
     */
    public String appName;
    /**
     * Application Server的ip地址
     */
    public String appServerIp;
    /**
     * Application Server的端口号
     */
    public int appServerPort;

    /**
     * 同步任务的唯一id, 这个id表示同步一个数据库的唯一标识
     */
    public String syncTaskId;
    /**
     * 针对这个mongodb 同步的开始时间, 在standby等待状态的时间是不计算的.
     */
    public long syncTaskStartTime = -1;
    /**
     * 这个同步任务内的唯一自增sequence.
     */
    public long syncTaskSequence = -1;
    /**
     * app – 应用直接产生的json格式数据, binlog- 对接的mysql binlog日志, oplog - 对接的MongoDB的oplog日志
     */
    public String sourceType;
    /**
     * 数据源database名称
     */
    public String dbName;
    /**
     * HBase database Name
     */
    public String hbaseDatabaseName = "";
    /**
     * 数据源表名称
     */
    public String tableName;
    /**
     * HBase TableName
     */
    public String hbaseTableName = "";
    /**
     * schema Version
     */
    public int schemaVersion = -1;
    /**
     * 消息本身携带的唯一标识
     */
    public String msgUuid;
    /**
     * 是否正常
     */
    public boolean isAbnormal = false;
    /**
     * 消息同步的开始时间
     */
    public long msgSyncStartTime = -1;
    /**
     * 消息同步结束事件
     */
    protected long msgSyncEndTime = -1;

    /**
     * 数据库中数据生效事件: 比如插入,修改
     */
    protected long dbEffectTime = -1;
    /**
     * 消息同步花费的时间,
     * 通过:
     * max(msgSyncStartTime) - min(msgSyncStartTime) == sum(msgSyncUsedTime)
     * group by
     * syncTaskStartTime
     * 来确认数据没有丢失.
     */
    public long msgSyncUsedTime = -1;

    /**
     * 消息的大小, String未经压缩
     */
    public int msgSize;

    /**
     * kafka topic
     */
    public String kafkaTopic;
    /**
     * kafka topic partition id
     */
    public int kafkaPartition = -1;
    /**
     * kafka topic partition offset偏移量
     */
    public long kafkaOffset = -1;

    /**
     * 对应mongodb中的op, 代表i-insert, u-update, d-delete
     * 对应mysql eventType, insert, update, delete, alter
     */
    public String eventType;

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public String getHbaseDatabaseName() {
        return hbaseDatabaseName;
    }

    public void setHbaseDatabaseName(String hbaseDatabaseName) {
        this.hbaseDatabaseName = hbaseDatabaseName;
    }

    public String getHbaseTableName() {
        return hbaseTableName;
    }

    public void setHbaseTableName(String hbaseTableName) {
        this.hbaseTableName = hbaseTableName;
    }

    public boolean isAbnormal() {
        return isAbnormal;
    }

    public void setAbnormal(boolean abnormal) {
        isAbnormal = abnormal;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAppServerIp() {
        return appServerIp;
    }

    public void setAppServerIp(String appServerIp) {
        this.appServerIp = appServerIp;
    }

    public int getAppServerPort() {
        return appServerPort;
    }

    public void setAppServerPort(int appServerPort) {
        this.appServerPort = appServerPort;
    }

    public String getSyncTaskId() {
        return syncTaskId;
    }

    public void setSyncTaskId(String syncTaskId) {
        this.syncTaskId = syncTaskId;
    }

    public long getSyncTaskStartTime() {
        return syncTaskStartTime;
    }

    public void setSyncTaskStartTime(long syncTaskStartTime) {
        this.syncTaskStartTime = syncTaskStartTime;
    }

    public long getSyncTaskSequence() {
        return syncTaskSequence;
    }

    public void setSyncTaskSequence(long syncTaskSequence) {
        this.syncTaskSequence = syncTaskSequence;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getMsgUuid() {
        return msgUuid;
    }

    public void setMsgUuid(String msgUuid) {
        this.msgUuid = msgUuid;
    }

    public long getMsgSyncStartTime() {
        return msgSyncStartTime;
    }

    public void setMsgSyncStartTime(long msgSyncStartTime) {
        this.msgSyncStartTime = msgSyncStartTime;
    }

    public long getMsgSyncUsedTime() {
        return msgSyncUsedTime;
    }

    public void setMsgSyncUsedTime(long msgSyncUsedTime) {
        this.msgSyncUsedTime = msgSyncUsedTime;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public int getKafkaPartition() {
        return kafkaPartition;
    }

    public void setKafkaPartition(int kafkaPartition) {
        this.kafkaPartition = kafkaPartition;
    }

    public long getKafkaOffset() {
        return kafkaOffset;
    }

    public void setKafkaOffset(long kafkaOffset) {
        this.kafkaOffset = kafkaOffset;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public long getMsgSyncEndTime() {
        return msgSyncEndTime;
    }

    public void setMsgSyncEndTime(long msgSyncEndTime) {
        this.msgSyncEndTime = msgSyncEndTime;
    }

    public long getDbEffectTime() {
        return dbEffectTime;
    }

    public void setDbEffectTime(long dbEffectTime) {
        this.dbEffectTime = dbEffectTime;
    }

    public PartitionStrategy getPartitionStrategy() {
        return partitionStrategy;
    }

    public void setPartitionStrategy(PartitionStrategy partitionStrategy) {
        this.partitionStrategy = partitionStrategy;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    @Override
    public String toString() {
        return "BaseDataJsonKey{" +
                "partitionStrategy='" + partitionStrategy + '\'' +
                "appName='" + appName + '\'' +
                ", appServerIp='" + appServerIp + '\'' +
                ", appServerPort=" + appServerPort +
                ", syncTaskId='" + syncTaskId + '\'' +
                ", syncTaskStartTime=" + syncTaskStartTime +
                ", syncTaskSequence=" + syncTaskSequence +
                ", sourceType='" + sourceType + '\'' +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", msgUuid='" + msgUuid + '\'' +
                ", msgSyncStartTime=" + msgSyncStartTime +
                ", msgSyncEndTime=" + msgSyncEndTime +
                ", dbEffectTime=" + dbEffectTime +
                ", msgSyncUsedTime=" + msgSyncUsedTime +
                ", msgSize=" + msgSize +
                ", kafkaTopic='" + kafkaTopic + '\'' +
                ", kafkaPartition=" + kafkaPartition +
                ", kafkaOffset=" + kafkaOffset +
                ", eventType='" + eventType + '\'' +
                ", hbaseDatabaseName='" + hbaseDatabaseName + '\'' +
                ", hbaseTableName='" + hbaseTableName + '\'' +
                ", isAbnormal'" + isAbnormal + '\'' +
                ", schemaVersion'" + schemaVersion + '\'' +
                '}';
    }
}
