package com.neighborhood.aka.laplace.estuary.web.bean;

/**
 * Created by john_liu on 2018/7/26.
 */

public class SnapshotRequestBean extends TaskRequestBean {
    private String syncTaskId;
    private String snapshotTaskId;
    private Long targetTimestamp;
    private int kafkaPartition;

    public String getSyncTaskId() {
        return syncTaskId;
    }

    public void setSyncTaskId(String syncTaskId) {
        this.syncTaskId = syncTaskId;
    }

    public String getSnapshotTaskId() {
        return snapshotTaskId;
    }

    public void setSnapshotTaskId(String snapshotTaskId) {
        this.snapshotTaskId = snapshotTaskId;
    }

    public Long getTargetTimestamp() {
        return targetTimestamp;
    }

    public void setTargetTimestamp(Long targetTimestamp) {
        this.targetTimestamp = targetTimestamp;
    }

    public int getKafkaPartition() {
        return kafkaPartition;
    }

    public void setKafkaPartition(int kafkaPartition) {
        this.kafkaPartition = kafkaPartition;
    }
}
