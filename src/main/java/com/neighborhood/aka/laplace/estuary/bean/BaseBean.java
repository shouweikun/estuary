package com.neighborhood.aka.laplace.estuary.bean;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.PrePersist;
import org.mongodb.morphia.annotations.Version;

import java.util.Date;

/**
 * Created by john_liu on 2018/2/2.
 */
public class BaseBean {
    @Id
    protected ObjectId id;

    protected Date createTime;
    protected Date lastChange;
    //@Version 为Entity提供一个乐观锁，动态加载，不需要设置值
    @Version
    private long version;

    public void setId(ObjectId id) {
        this.id = id;
    }

    public ObjectId getId() {
        return id;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public Date getLastChange() {
        return lastChange;
    }

    @Override
    public String toString() {
        return "BaseBean{" +
                "id=" + id +
                ", createTime=" + createTime +
                ", lastChange=" + lastChange +
                ", version=" + version +
                '}';
    }

    public String getHexString() {
        if (id != null) {
            return id.toHexString();
        }
        return null;
    }
}
