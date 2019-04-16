package com.neighborhood.aka.laplace.estuary.web.bean;

/**
 * Created by john_liu on 2019/3/15.
 */
public class Mongo2HdfsTaskRequestBean extends TaskRequestBean {

    private boolean isMulti = false;

    private Mongo2HdfsRunningInfoRequestBean mongo2HdfsRunningInfo;
    private MongoSourceRequestBean mongoSource;
    private HdfsSinkRequestBean hdfsSink;

    public boolean isMulti() {
        return isMulti;
    }

    public void setMulti(boolean multi) {
        isMulti = multi;
    }

    public Mongo2HdfsRunningInfoRequestBean getMongo2HdfsRunningInfo() {
        return mongo2HdfsRunningInfo;
    }

    public void setMongo2HdfsRunningInfo(Mongo2HdfsRunningInfoRequestBean mongo2HdfsRunningInfo) {
        this.mongo2HdfsRunningInfo = mongo2HdfsRunningInfo;
    }

    public MongoSourceRequestBean getMongoSource() {
        return mongoSource;
    }

    public void setMongoSource(MongoSourceRequestBean mongoSource) {
        this.mongoSource = mongoSource;
    }

    public HdfsSinkRequestBean getHdfsSink() {
        return hdfsSink;
    }

    public void setHdfsSink(HdfsSinkRequestBean hdfsSink) {
        this.hdfsSink = hdfsSink;
    }
}
