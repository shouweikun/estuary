package com.neighborhood.aka.laplace.estuary.web.bean;

/**
 * Created by john_liu on 2019/1/17.
 */
public class Mysql2MysqlRequestBean extends TaskRequestBean {

    private MysqlSourceBean mysqlSourceBean;
    private MysqlSinkBean mysqlSinkBean;
    private Mysql2MysqlRunningInfoBean mysql2MysqlRunningInfoBean;
    private SdaRequestBean sdaBean;

    public SdaRequestBean getSdaBean() {
        return sdaBean;
    }

    public void setSdaBean(SdaRequestBean sdaBean) {
        this.sdaBean = sdaBean;
    }

    public MysqlSourceBean getMysqlSourceBean() {
        return mysqlSourceBean;
    }

    public void setMysqlSourceBean(MysqlSourceBean mysqlSourceBean) {
        this.mysqlSourceBean = mysqlSourceBean;
    }

    public MysqlSinkBean getMysqlSinkBean() {
        return mysqlSinkBean;
    }

    public void setMysqlSinkBean(MysqlSinkBean mysqlSinkBean) {
        this.mysqlSinkBean = mysqlSinkBean;
    }

    public Mysql2MysqlRunningInfoBean getMysql2MysqlRunningInfoBean() {
        return mysql2MysqlRunningInfoBean;
    }

    public void setMysql2MysqlRunningInfoBean(Mysql2MysqlRunningInfoBean mysql2MysqlRunningInfoBean) {
        this.mysql2MysqlRunningInfoBean = mysql2MysqlRunningInfoBean;
    }
}
