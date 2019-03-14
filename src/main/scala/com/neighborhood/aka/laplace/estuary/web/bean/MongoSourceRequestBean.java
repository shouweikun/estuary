package com.neighborhood.aka.laplace.estuary.web.bean;

import java.util.List;

/**
 * Created by john_liu on 2019/3/7.
 */
public class MongoSourceRequestBean {

    private String authMechanism;
    private List<MongoCredentialRequestBean> mongoCredentials;
    private List<String> hosts;
    private int port;
    private String[] concernedNs;
    private String[] ignoredNs;


    public String getAuthMechanism() {
        return authMechanism;
    }

    public void setAuthMechanism(String authMechanism) {
        this.authMechanism = authMechanism;
    }


    public List<MongoCredentialRequestBean> getMongoCredentials() {
        return mongoCredentials;
    }

    public void setMongoCredentials(List<MongoCredentialRequestBean> mongoCredentials) {
        this.mongoCredentials = mongoCredentials;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String[] getConcernedNs() {
        return concernedNs;
    }

    public void setConcernedNs(String[] concernedNs) {
        this.concernedNs = concernedNs;
    }

    public String[] getIgnoredNs() {
        return ignoredNs;
    }

    public void setIgnoredNs(String[] ignoredNs) {
        this.ignoredNs = ignoredNs;
    }
}