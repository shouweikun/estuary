package com.neighborhood.aka.laplace.estuary.web.bean;



/**
 * Created by john_liu on 2019/1/17.
 */
public class MysqlSinkBean {

   private MysqlCredentialRequestBean credential;
    private boolean isAutoCommit;
    private long connectionTimeout;
    private int maximumPoolSize;

    public MysqlCredentialRequestBean getCredential() {
        return credential;
    }

    public void setCredential(MysqlCredentialRequestBean credential) {
        this.credential = credential;
    }

    public boolean isAutoCommit() {
        return isAutoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        isAutoCommit = autoCommit;
    }

    public long getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(long connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
    }
}
