package com.neighborhood.aka.laplace.estuary.web.bean;

/**
 * Created by john_liu on 2019/1/18.
 */
public class MysqlCredentialRequestBean {


    private String address;

    /**
     * 服务器端口号
     */
    private int port;

    /**
     * 服务器用户名
     */
    private String username;

    /**
     * 服务器密码
     */
    private String password;

    /**
     * 服务器密码
     */
    private String defaultDatabase;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public void setDefaultDatabase(String defaultDatabase) {
        this.defaultDatabase = defaultDatabase;
    }
}
