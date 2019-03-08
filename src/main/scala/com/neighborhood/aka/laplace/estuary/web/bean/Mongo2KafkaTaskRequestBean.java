package com.neighborhood.aka.laplace.estuary.web.bean;

/**
 * Created by john_liu on 2019/3/7.
 *
 * @author neighborhood.aka.lapalce
 */
public class Mongo2KafkaTaskRequestBean extends TaskRequestBean{

    private Mongo2KafkaRunningInfoRequestBean mongo2KafkaRunningInfoRequestBean;
    private MongoSourceRequestBean mongoSourceRequestBean;
    private KafkaSinkRequestBean kafkaSinkRequestBean;

    public Mongo2KafkaRunningInfoRequestBean getMongo2KafkaRunningInfoRequestBean() {
        return mongo2KafkaRunningInfoRequestBean;
    }

    public void setMongo2KafkaRunningInfoRequestBean(Mongo2KafkaRunningInfoRequestBean mongo2KafkaRunningInfoRequestBean) {
        this.mongo2KafkaRunningInfoRequestBean = mongo2KafkaRunningInfoRequestBean;
    }

    public MongoSourceRequestBean getMongoSourceRequestBean() {
        return mongoSourceRequestBean;
    }

    public void setMongoSourceRequestBean(MongoSourceRequestBean mongoSourceRequestBean) {
        this.mongoSourceRequestBean = mongoSourceRequestBean;
    }

    public KafkaSinkRequestBean getKafkaSinkRequestBean() {
        return kafkaSinkRequestBean;
    }

    public void setKafkaSinkRequestBean(KafkaSinkRequestBean kafkaSinkRequestBean) {
        this.kafkaSinkRequestBean = kafkaSinkRequestBean;
    }
}
