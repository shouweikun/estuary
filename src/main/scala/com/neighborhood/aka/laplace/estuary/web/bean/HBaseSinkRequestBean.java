package com.neighborhood.aka.laplace.estuary.web.bean;

/**
 * Created by john_liu on 2019/3/15.
 */
public class HBaseSinkRequestBean {
   private String HbaseZookeeperQuorum;
    private String  HabseZookeeperPropertyClientPort;

    public String getHbaseZookeeperQuorum() {
        return HbaseZookeeperQuorum;
    }

    public void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
        HbaseZookeeperQuorum = hbaseZookeeperQuorum;
    }


    public String getHabseZookeeperPropertyClientPort() {
        return HabseZookeeperPropertyClientPort;
    }

    public void setHabseZookeeperPropertyClientPort(String habseZookeeperPropertyClientPort) {
        HabseZookeeperPropertyClientPort = habseZookeeperPropertyClientPort;
    }
}
