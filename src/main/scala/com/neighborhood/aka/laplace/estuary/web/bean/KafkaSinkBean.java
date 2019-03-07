package com.neighborhood.aka.laplace.estuary.web.bean;

import java.util.Map;

/**
 * Created by john_liu on 2019/3/7.
 */
public class KafkaSinkBean {
    private String bootstrapServers;
    private String topic;
    private String ddlTopic;
    private Map<String, String> specificTopics;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getDdlTopic() {
        return ddlTopic;
    }

    public void setDdlTopic(String ddlTopic) {
        this.ddlTopic = ddlTopic;
    }

    public Map<String, String> getSpecificTopics() {
        return specificTopics;
    }

    public void setSpecificTopics(Map<String, String> specificTopics) {
        this.specificTopics = specificTopics;
    }
}
