package com.neighborhood.aka.laplace.estuary.bean;

/**
 * Created by zhujinlong on 17-3-31.
 */
public class MysqlCredentialBean {
    /**
     * 服务器地址
     */
    public String address;
    /**
     * 服务器端口号
     */
    public int port;
    /**
     * 服务器用户名
     */
    public String username;
    /**
     * 服务器密码
     */
    public String password;
    /**
     * 数据库名称
     */
    public String database="";
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

    @Override
    public String toString() {
        return "MysqlCredentialBean{" +
                "address='" + address + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                '}';
    }
}
