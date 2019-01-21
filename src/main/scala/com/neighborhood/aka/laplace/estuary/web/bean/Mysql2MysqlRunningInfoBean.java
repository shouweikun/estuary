package com.neighborhood.aka.laplace.estuary.web.bean;

import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy;

import java.util.List;
import java.util.Map;

/**
 * Created by john_liu on 2019/1/17.
 */
public class Mysql2MysqlRunningInfoBean {

    private String syncTaskId;
    private String offsetZkServers;
    private long syncStartTime;
    private EntryPosition startPosition;
    private List<String> mysqlDatabaseNameList;
    private long batchThreshold = 1;
    private boolean schemaComponentIsOn;
    private boolean isNeedExecuteDDL;
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
    private String mappingFormatName;

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

    public String getOffsetZkServers() {
        return offsetZkServers;
    }

    public void setOffsetZkServers(String offsetZkServers) {
        this.offsetZkServers = offsetZkServers;
    }

    public long getSyncStartTime() {
        return syncStartTime;
    }

    public void setSyncStartTime(long syncStartTime) {
        this.syncStartTime = syncStartTime;
    }

    public EntryPosition getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(EntryPosition startPosition) {
        this.startPosition = startPosition;
    }

    public List<String> getMysqlDatabaseNameList() {
        return mysqlDatabaseNameList;
    }

    public void setMysqlDatabaseNameList(List<String> mysqlDatabaseNameList) {
        this.mysqlDatabaseNameList = mysqlDatabaseNameList;
    }

    public long getBatchThreshold() {
        return batchThreshold;
    }

    public void setBatchThreshold(long batchThreshold) {
        this.batchThreshold = batchThreshold;
    }

    public boolean isSchemaComponentIsOn() {
        return schemaComponentIsOn;
    }

    public void setSchemaComponentIsOn(boolean schemaComponentIsOn) {
        this.schemaComponentIsOn = schemaComponentIsOn;
    }

    public boolean isNeedExecuteDDL() {
        return isNeedExecuteDDL;
    }

    public void setNeedExecuteDDL(boolean needExecuteDDL) {
        isNeedExecuteDDL = needExecuteDDL;
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
