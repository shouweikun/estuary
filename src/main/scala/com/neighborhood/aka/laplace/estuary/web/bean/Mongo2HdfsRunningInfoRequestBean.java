package com.neighborhood.aka.laplace.estuary.web.bean;

import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy;

import java.util.Map;

/**
 * Created by john_liu on 2019/3/15.
 */
public class Mongo2HdfsRunningInfoRequestBean {

    private String syncTaskId;
    private long syncStartTime;
    private int mongoTsSecond = (int)(System.currentTimeMillis()/1000);
    private int mongoTsInc = 0;
    private long batchThreshold = 1;
    private boolean isCounting = true;
    private boolean isCosting = true;
    private boolean isProfiling = true;
    private boolean isPowerAdapted = true;
    private PartitionStrategy partitionStrategy = PartitionStrategy.PRIMARY_KEY;
    private Map<String, String> controllerNameToLoad;
    private Map<String, String> sinkerNameToLoad;
    private Map<String, String> fetcherNameToLoad;
    private Map<String, String> batcherNameToLoad;
    private int batcherNum = 23;
    private int sinkerNum = 23;
    private String mappingFormatName;
    private String OffsetZookeeperServers;

    public String getOffsetZookeeperServers() {
        return OffsetZookeeperServers;
    }

    public void setOffsetZookeeperServers(String offsetZookeeperServers) {
        OffsetZookeeperServers = offsetZookeeperServers;
    }

    public int getSinkerNum() {
        return sinkerNum;
    }

    public void setSinkerNum(int sinkerNum) {
        this.sinkerNum = sinkerNum;
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


    public String getMappingFormatName() {
        return mappingFormatName;
    }

    public void setMappingFormatName(String mappingFormatName) {
        this.mappingFormatName = mappingFormatName;
    }

    public String getSyncTaskId() {
        return syncTaskId;
    }

    public void setSyncTaskId(String syncTaskId) {
        this.syncTaskId = syncTaskId;
    }


    public long getSyncStartTime() {
        return syncStartTime;
    }

    public void setSyncStartTime(long syncStartTime) {
        this.syncStartTime = syncStartTime;
    }




    public long getBatchThreshold() {
        return batchThreshold;
    }

    public void setBatchThreshold(long batchThreshold) {
        this.batchThreshold = batchThreshold;
    }

    public boolean isCounting() {
        return isCounting;
    }

    public void setCounting(boolean counting) {
        isCounting = counting;
    }

    public boolean isCosting() {
        return isCosting;
    }

    public void setCosting(boolean costing) {
        isCosting = costing;
    }

    public boolean isProfiling() {
        return isProfiling;
    }

    public void setProfiling(boolean profiling) {
        isProfiling = profiling;
    }

    public boolean isPowerAdapted() {
        return isPowerAdapted;
    }

    public void setPowerAdapted(boolean powerAdapted) {
        isPowerAdapted = powerAdapted;
    }

    public PartitionStrategy getPartitionStrategy() {
        return partitionStrategy;
    }

    public void setPartitionStrategy(PartitionStrategy partitionStrategy) {
        this.partitionStrategy = partitionStrategy;
    }

    public Map<String, String> getControllerNameToLoad() {
        return controllerNameToLoad;
    }

    public void setControllerNameToLoad(Map<String, String> controllerNameToLoad) {
        this.controllerNameToLoad = controllerNameToLoad;
    }

    public Map<String, String> getSinkerNameToLoad() {
        return sinkerNameToLoad;
    }

    public void setSinkerNameToLoad(Map<String, String> sinkerNameToLoad) {
        this.sinkerNameToLoad = sinkerNameToLoad;
    }

    public Map<String, String> getFetcherNameToLoad() {
        return fetcherNameToLoad;
    }

    public void setFetcherNameToLoad(Map<String, String> fetcherNameToLoad) {
        this.fetcherNameToLoad = fetcherNameToLoad;
    }


    public Map<String, String> getBatcherNameToLoad() {
        return batcherNameToLoad;
    }

    public void setBatcherNameToLoad(Map<String, String> batcherNameToLoad) {
        this.batcherNameToLoad = batcherNameToLoad;
    }

    public int getBatcherNum() {
        return batcherNum;
    }

    public void setBatcherNum(int batcherNum) {
        this.batcherNum = batcherNum;
    }
}
