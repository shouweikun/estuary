package com.neighborhood.aka.laplace.estuary.web.bean;

/**
 * Created by john_liu on 2019/3/15.
 */
public class Mongo2HdfsTaskRequestBean extends TaskRequestBean {

    private boolean isMulti = false;

    private Mongo2HBaseRunningInfoRequestBean mongo2HBaseRunningInfo;
    private MongoSourceRequestBean mongoSource;
    private HBaseSinkRequestBean hbaseSink;

    public boolean isMulti() {
        return isMulti;
    }

    public void setMulti(boolean multi) {
        isMulti = multi;
    }

    public Mongo2HBaseRunningInfoRequestBean getMongo2HBaseRunningInfo() {
        return mongo2HBaseRunningInfo;
    }

    public void setMongo2HBaseRunningInfo(Mongo2HBaseRunningInfoRequestBean mongo2HBaseRunningInfo) {
        this.mongo2HBaseRunningInfo = mongo2HBaseRunningInfo;
    }

    public MongoSourceRequestBean getMongoSource() {
        return mongoSource;
    }

    public void setMongoSource(MongoSourceRequestBean mongoSource) {
        this.mongoSource = mongoSource;
    }

    public HBaseSinkRequestBean getHbaseSink() {
        return hbaseSink;
    }

    public void setHbaseSink(HBaseSinkRequestBean hbaseSink) {
        this.hbaseSink = hbaseSink;
    }
}
