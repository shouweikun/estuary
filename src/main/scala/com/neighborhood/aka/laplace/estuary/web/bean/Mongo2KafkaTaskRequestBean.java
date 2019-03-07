package com.neighborhood.aka.laplace.estuary.web.bean;

/**
 * Created by john_liu on 2019/3/7.
 *
 * @author neighborhood.aka.lapalce
 */
public class Mongo2KafkaTaskRequestBean {

    private Mongo2KafkaRunningInfoBean mongo2KafkaRunningInfoBean;
    private MongoSourceBean mongoSourceBean;
    private KafkaSinkBean kafkaSinkBean;

    public Mongo2KafkaRunningInfoBean getMongo2KafkaRunningInfoBean() {
        return mongo2KafkaRunningInfoBean;
    }

    public void setMongo2KafkaRunningInfoBean(Mongo2KafkaRunningInfoBean mongo2KafkaRunningInfoBean) {
        this.mongo2KafkaRunningInfoBean = mongo2KafkaRunningInfoBean;
    }

    public MongoSourceBean getMongoSourceBean() {
        return mongoSourceBean;
    }

    public void setMongoSourceBean(MongoSourceBean mongoSourceBean) {
        this.mongoSourceBean = mongoSourceBean;
    }

    public KafkaSinkBean getKafkaSinkBean() {
        return kafkaSinkBean;
    }

    public void setKafkaSinkBean(KafkaSinkBean kafkaSinkBean) {
        this.kafkaSinkBean = kafkaSinkBean;
    }
}
