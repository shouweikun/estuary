package com.neighborhood.aka.laplace.estuary.mongo.source;

/**
 * Created by z on 17-3-31.
 */
public class MongoOffset {
    /**
     * 对应mongodb 中的ts字段中的秒,
     */
    private int mongoTsSecond = 0;
    /**
     * 对应mongodb 中的ts字段中的inc, mongoTsSecond与mongoTsInc组成一个Ops log中的唯一值.
     */
    private int mongoTsInc = 0;
    /**
     * 要同步的数据量, 如果为-1表示不限制
     */
    private int mongoLimit = Integer.MAX_VALUE;

    public int getMongoTsSecond() {
        return mongoTsSecond;
    }

    public void setMongoTsSecond(int mongoTsSecond) {
        this.mongoTsSecond = mongoTsSecond;
    }

    public int getMongoTsInc() {
        return mongoTsInc;
    }

    public void setMongoTsInc(int mongoTsInc) {
        this.mongoTsInc = mongoTsInc;
    }

    public int getMongoLimit() {
        return mongoLimit;
    }

    public void setMongoLimit(int mongoLimit) {
        if (mongoLimit > 0) {
            this.mongoLimit = mongoLimit;
        }
    }

    @Override
    public String toString() {
        return "MongoOffset{" +
                "mongoTsSecond=" + mongoTsSecond +
                ", mongoTsInc=" + mongoTsInc +
                ", mongoLimit=" + mongoLimit +
                '}';
    }
}
