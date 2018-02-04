package com.neighborhood.aka.laplace.estuary.bean;

import org.mongodb.morphia.annotations.Embedded;

import java.nio.charset.Charset;

/**
 * Created by john_liu on 2018/2/2.
 */
public class MysqlBean {
    /**
     * 主服务器（服务器地址，端口，用户名，密码）
     */
    @Embedded
    private MysqlCredentialBean master;
    /**
     * 从服务器
     */
    @Embedded
    private MysqlCredentialBean standby;

    /**
     * 过滤条件.
     */
    private String filterPattern;


    private String filterBlackPattern;
    /**
     * 默认的channel sotimeout, 对应MysqlConnector soTimeout
     */
    private int defaultConnectionTimeoutInSeconds = 30;
    /**
     */
    private int receiveBufferSize = 16 * 1024 * 1024;
    /**
     */
    private int sendBufferSize = 16 * 1024;

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    public void setSendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    protected byte connectionCharsetNumber = (byte) 33;
    protected Charset connectionCharset = Charset.forName("UTF-8");

    public byte getConnectionCharsetNumber() {
        return connectionCharsetNumber;
    }

    public void setConnectionCharsetNumber(byte connectionCharsetNumber) {
        this.connectionCharsetNumber = connectionCharsetNumber;
    }

    public Charset getConnectionCharset() {
        return connectionCharset;
    }

    public void setConnectionCharset(Charset connectionCharset) {
        this.connectionCharset = connectionCharset;
    }

    public MysqlCredentialBean getMaster() {
        return master;
    }

    public void setMaster(MysqlCredentialBean master) {
        this.master = master;
    }

    public MysqlCredentialBean getStandby() {
        return standby;
    }

    public void setStandby(MysqlCredentialBean standby) {
        this.standby = standby;
    }

    public String getFilterPattern() {
        return filterPattern;
    }

    public String getEventBlackFilter() {
        return filterBlackPattern;
    }

    public void setFilterBlackPattern(String filterBlackPattern) {
        this.filterBlackPattern = filterBlackPattern;
    }

    public String getFilterBlackPattern() {
        return filterBlackPattern;
    }

    public void setFilterPattern(String filterPattern) {
        this.filterPattern = filterPattern;
    }

    public int getDefaultConnectionTimeoutInSeconds() {
        return defaultConnectionTimeoutInSeconds;
    }

    public void setDefaultConnectionTimeoutInSeconds(int defaultConnectionTimeoutInSeconds) {
        this.defaultConnectionTimeoutInSeconds = defaultConnectionTimeoutInSeconds;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public void setReceiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

//    @Override
//        public String toString() {
//                return ToStringBuilder.reflectionToString(this);
//        }

    @Override
    public String toString() {
        return "MysqlBean{" +
                "master=" + master +
                ", standby=" + standby +
                ", filterPattern='" + filterPattern + '\'' +
                ", filterBlackPattern='" + filterBlackPattern + '\'' +
                ", defaultConnectionTimeoutInSeconds=" + defaultConnectionTimeoutInSeconds +
                ", receiveBufferSize=" + receiveBufferSize +
                '}';
    }
}
