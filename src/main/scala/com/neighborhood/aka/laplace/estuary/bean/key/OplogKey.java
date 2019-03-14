package com.neighborhood.aka.laplace.estuary.bean.key;

import com.neighborhood.aka.laplace.estuary.mongo.source.Oplog;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * Created by z on 17-3-31.
 * todo
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OplogKey extends BaseDataJsonKey {
    public OplogKey() {
        super();
    }

    public OplogKey(Oplog oplog) {
        this.setMongoOpsUuid(oplog.getId());
        this.sourceType = "oplog";
        this.msgUuid = Long.toHexString(oplog.getId());
        this.dbName = oplog.getDbName();
        this.tableName = oplog.getTableName();

        this.setMongoTsSecond(oplog.getTimestamp().getTime());
        this.setMongoTsInc(oplog.getTimestamp().getInc());
        this.eventType = oplog.getOperateType();

        // 这边是秒,需要处理
        setDbEffectTime(((long)this.mongoTsSecond)*1000);
    }

    /**
     * 对mongodb集群的标识
     */
    private String clusterId;

    /**
     * 对应mongodb 中的h字段, mongodb中的唯一值.
     */
    private long mongoOpsUuid = -1;
    /**
     * 对应mongodb 中的ts字段中的秒,
     */
    private int mongoTsSecond = -1;
    /**
     * 对应mongodb 中的ts字段中的inc, mongoTsSecond与mongoTsInc组成一个Ops log中的唯一值.
     */
    private int mongoTsInc = -1;

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public long getMongoOpsUuid() {
        return mongoOpsUuid;
    }

    public void setMongoOpsUuid(long mongoOpsUuid) {
        this.mongoOpsUuid = mongoOpsUuid;
    }

    public int getMongoTsSecond() {
        return mongoTsSecond;
    }

    public void setMongoTsSecond(int mongoTsSecond) {
        this.mongoTsSecond = mongoTsSecond;
    }

    public int getMongoTsInc() {
        return mongoTsInc;
    }

    public void setMongoTsInc(int mongoTsInc) {
        this.mongoTsInc = mongoTsInc;
    }

    @Override
    public String toString() {
        return "OplogKey{" +
            "clusterId='" + clusterId + '\'' +
            ", mongoOpsUuid=" + mongoOpsUuid +
            ", mongoTsSecond=" + mongoTsSecond +
            ", mongoTsInc=" + mongoTsInc +
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
            '}';
    }
}
