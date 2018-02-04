package com.neighborhood.aka.laplace.estuary.bean;

import com.alibaba.otter.canal.protocol.position.EntryPosition;

/**
 * Created by john_liu on 2018/2/2.
 */
public class MysqlSyncTaskBean extends MysqlBean {
    /**
     * 主机对应的entryPosition
     */
    private EntryPosition masterPostion;
    /**
     * 从机对应的entryPosition
     */
    private EntryPosition slavePostion;

    public EntryPosition getMasterPostion() {
        return masterPostion;
    }

    public void setMasterPostion(EntryPosition masterPostion) {
        this.masterPostion = masterPostion;
    }

    public EntryPosition getSlavePostion() {
        return slavePostion;
    }

    public void setSlavePostion(EntryPosition slavePostion) {
        this.slavePostion = slavePostion;
    }
}
