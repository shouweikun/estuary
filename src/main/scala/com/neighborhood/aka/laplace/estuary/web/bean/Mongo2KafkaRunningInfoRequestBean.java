package com.neighborhood.aka.laplace.estuary.web.bean;

import com.neighborhood.aka.laplace.estuary.bean.key.PartitionStrategy;

import java.util.List;
import java.util.Map;

/**
 * Created by john_liu on 2019/3/7.
 */
public class Mongo2KafkaRunningInfoRequestBean {


    private String syncTaskId;
    private long syncStartTime;
    private int mongoTsSecond = (int)(System.currentTimeMillis()/1000);
    private int mongoTsInc = 0;
    private List<String> mysqlDatabaseNameList;
    private long batchThreshold = 1;
    private boolean schemaComponentIsOn;
    private boolean isNeedExecuteDDL;
    private boolean isCounting = true;
    private boolean isCosting = true;
    private boolean isProfiling = true;
    private boolean isPowerAdapted = true;
    private boolean checkSinkSchema = false;
    private PartitionStrategy partitionStrategy = PartitionStrategy.PRIMARY_KEY;
    private Map<String, String> controllerNameToLoad;
    private Map<String, String> sinkerNameToLoad;
    private Map<String, String> fetcherNameToLoad;
    private Map<String, String> batcherNameToLoad;
    private int batcherNum = 23;
    private int sinkerNum = 23;
    private String mappingFormatName;

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

    public boolean isCheckSinkSchema() {
        return checkSinkSchema;
    }

    public void setCheckSinkSchema(boolean checkSinkSchema) {
        this.checkSinkSchema = checkSinkSchema;
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
