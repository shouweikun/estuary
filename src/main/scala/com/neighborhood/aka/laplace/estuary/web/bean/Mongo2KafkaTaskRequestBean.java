package com.neighborhood.aka.laplace.estuary.web.bean;

/**
 * Created by john_liu on 2019/3/7.
 *
 * @author neighborhood.aka.lapalce
 */
public class Mongo2KafkaTaskRequestBean extends TaskRequestBean{

    private Mongo2KafkaRunningInfoRequestBean mongo2KafkaRunningInfo;
    private MongoSourceRequestBean mongoSource;
    private KafkaSinkRequestBean kafkaSink;

    public Mongo2KafkaRunningInfoRequestBean getMongo2KafkaRunningInfo() {
        return mongo2KafkaRunningInfo;
    }

    public void setMongo2KafkaRunningInfo(Mongo2KafkaRunningInfoRequestBean mongo2KafkaRunningInfo) {
        this.mongo2KafkaRunningInfo = mongo2KafkaRunningInfo;
    }

    public MongoSourceRequestBean getMongoSource() {
        return mongoSource;
    }

    public void setMongoSource(MongoSourceRequestBean mongoSource) {
        this.mongoSource = mongoSource;
    }

    public KafkaSinkRequestBean getKafkaSink() {
        return kafkaSink;
    }

    public void setKafkaSink(KafkaSinkRequestBean kafkaSink) {
        this.kafkaSink = kafkaSink;
    }
}
